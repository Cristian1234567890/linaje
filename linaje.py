"""
Generador de linaje (impala sql) - python
Salida: lista de dicts (json-serializable) con los campos:
id, consulta, tabla_origen, tabla_destino, campo_origen, campo_destino, transformacion_aplicada, recomendaciones

Notas:
- todo en minÃºsculas
- heurÃ­stico: intenta manejar insert/select, create as select, with ... insert ... as
- si detecta '*' o 'table.*' genera relaciones a nivel tabla
- comentarÃ© el cÃ³digo paso a paso (en espaÃ±ol)
"""

import re
import json
import uuid
import os
import pandas as pd

# -------------------------
# Helpers de anÃ¡lisis lÃ©xico simples (manejan parÃ©ntesis y comillas)
# -------------------------

def normalize_sql(sql: str) -> str:
    """Normaliza: elimina comentarios, pasa a minúsculas y colapsa espacios."""
    # eliminar comentarios de bloque y de línea
    s = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    s = re.sub(r'--.*?(?=\r?\n|$)', ' ', s)
    # normalizar espacios y mayúsculas
    s = s.strip()
    s = s.lower()
    s = re.sub(r'\s+', ' ', s)
    return s
def top_level_split(s: str, delimiter: str=',') -> list:
    """
    Divide una cadena por delimitador pero solo en nivel superior (depth==0).
    Maneja comillas simples y dobles y parÃ©ntesis.
    """
    parts = []
    cur = []
    depth = 0
    in_s = False
    in_d = False
    i = 0
    while i < len(s):
        ch = s[i]
        # manejo de comillas (no interpretamos escapes)
        if ch == "'" and not in_d:
            in_s = not in_s
            cur.append(ch); i += 1; continue
        if ch == '"' and not in_s:
            in_d = not in_d
            cur.append(ch); i += 1; continue
        if in_s or in_d:
            cur.append(ch); i += 1; continue
        if ch == '(':
            depth += 1
            cur.append(ch); i += 1; continue
        if ch == ')':
            if depth > 0:
                depth -= 1
            cur.append(ch); i += 1; continue
        if ch == delimiter and depth == 0:
            parts.append(''.join(cur).strip())
            cur = []
            i += 1
            continue
        cur.append(ch)
        i += 1
    last = ''.join(cur).strip()
    if last:
        parts.append(last)
    return parts

def find_top_level_keyword(s: str, keyword: str, start: int=0) -> int:
    """
    Busca la posiciÃ³n de la palabra keyword a nivel top (no dentro de parÃ©ntesis ni comillas).
    Retorna Ã­ndice o -1 si no encuentra.
    """
    keyword = keyword.lower()
    i = start
    depth = 0
    in_s = False
    in_d = False
    L = len(s)
    while i < L:
        ch = s[i]
        if ch == "'" and not in_d:
            in_s = not in_s; i += 1; continue
        if ch == '"' and not in_s:
            in_d = not in_d; i += 1; continue
        if in_s or in_d:
            i += 1; continue
        if ch == '(':
            depth += 1; i += 1; continue
        if ch == ')':
            if depth > 0: depth -= 1
            i += 1; continue
        # si estamos en nivel top, probar si keyword encaja acÃ¡
        if depth == 0:
            if s.startswith(keyword, i):
                # verificar fronteras de palabra (-1 o no alfanumÃ©rico antes y despuÃ©s)
                before = s[i-1] if i-1 >= 0 else ' '
                after_pos = i + len(keyword)
                after = s[after_pos] if after_pos < L else ' '
                if (not before.isalnum()) and (not after.isalnum()):
                    return i
        i += 1
    return -1

def split_statements_top_level(sql: str) -> list:
    """Divide mÃºltiples sentencias separadas por ; a nivel top."""
    parts = []
    cur = []
    depth = 0
    in_s = False
    in_d = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_d:
            in_s = not in_s; cur.append(ch); i += 1; continue
        if ch == '"' and not in_s:
            in_d = not in_d; cur.append(ch); i += 1; continue
        if in_s or in_d:
            cur.append(ch); i += 1; continue
        if ch == '(':
            depth += 1; cur.append(ch); i += 1; continue
        if ch == ')':
            if depth > 0: depth -= 1
            cur.append(ch); i += 1; continue
        if ch == ';' and depth == 0:
            stmt = ''.join(cur).strip()
            if stmt:
                parts.append(stmt)
            cur = []
            i += 1
            continue
        cur.append(ch)
        i += 1
    last = ''.join(cur).strip()
    if last:
        parts.append(last)
    return parts

# -------------------------
# ExtracciÃ³n de partes principales
# -------------------------

def extract_select_range(stmt: str) -> tuple:
    """
    Retorna (select_start_idx, select_end_idx) donde select_end es la posiciÃ³n del 'from' top-level correspondiente.
    Si no hay 'select' o 'from' adecuados retorna (-1, -1).
    """
    sel_pos = find_top_level_keyword(stmt, 'select', 0)
    if sel_pos == -1:
        return -1, -1
    # buscamos el FROM top-level que siga
    from_pos = find_top_level_keyword(stmt, 'from', sel_pos + len('select'))
    if from_pos == -1:
        return sel_pos, -1
    return sel_pos, from_pos

def extract_select_items(stmt: str) -> list:
    """
    Extrae la lista de items del SELECT (entre select y from) en nivel top y los separa por comas top-level.
    """
    sel_pos, from_pos = extract_select_range(stmt)
    if sel_pos == -1 or from_pos == -1:
        return []
    select_str = stmt[sel_pos + len('select'): from_pos].strip()
    items = top_level_split(select_str, delimiter=',')
    return [it.strip() for it in items if it.strip()]

def extract_from_clause(stmt: str) -> str:
    """
    Extrae el fragmento 'from ...' hasta la siguiente palabra clave top-level (where, group, order, having, limit, union).
    """
    sel_pos, from_pos = extract_select_range(stmt)
    if from_pos == -1:
        return ''
    start = from_pos + len('from')
    # buscar prÃ³xima palabra clave top-level
    keywords = ['where', 'group', 'having', 'order', 'limit', 'union', 'insert', ';']
    next_pos = len(stmt)
    for k in keywords:
        p = find_top_level_keyword(stmt, k, start)
        if p != -1 and p < next_pos:
            next_pos = p
    return stmt[start:next_pos].strip()

def extract_tables_from_from_clause(from_clause: str) -> list:
    """
    Extrae nombres de tablas calificados tipo schema.tabla desde el from/join fragment.
    Devuelve lista de (tabla_completa, alias_o_none).
    """
    res = []
    # separa por joins y comas top-level
    # reemplazamos ' join ' por ', ' para poder splittear por comas top-level
    # pero mantendremos parÃ©ntesis manejados por top_level_split
    # para simplificar, covertimos las palabras join en ',' y luego top_level_split por ','
    # sin embargo, debemos preservar subqueries entre parÃ©ntesis; top_level_split lo harÃ¡ bien.
    # sustituimos palabras clave join por comas solo a nivel texto (no afecta parÃ©ntesis)
    norm = re.sub(r'\b(left|right|inner|outer|full)\s+join\b', ' join', from_clause)
    # ahora sustituir ' join ' y ' on ' por comas para separar bloques
    tmp = re.sub(r'\bjoin\b', ',', norm)
    tmp = re.sub(r'\bon\b.*', '', tmp)  # eliminar condiciones ON (simplifica)
    parts = top_level_split(tmp, delimiter=',')
    reserved = set(['select','from','where','join','on','left','right','inner','outer','full','as','group','order','by','limit','union','insert','into','table','with','stored','parquet','if','not','exists'])
    for p in parts:
        p = p.strip()
        # buscar pattern schema.table (ej. sbani.tablacontacta)
        m = re.search(r'([a-z0-9_]+\.[a-z0-9_]+)', p)
        tabla = None
        alias = None
        if m:
            tabla = m.group(1)
            # buscar alias (as alias o simple alias)
            m2 = re.search(r'\b' + re.escape(tabla) + r'\b\s+(?:as\s+)?([a-z0-9_]+)', p)
            if m2:
                alias = m2.group(1)
        else:
            # intentar detectar nombre de tabla sin esquema (primer token no reservado)
            mb = re.search(r'\b([a-z_][a-z0-9_]*)\b', p)
            if mb:
                cand = mb.group(1)
                if cand not in reserved:
                    tabla = cand
                    m2 = re.search(r'\b' + re.escape(tabla) + r'\b\s+(?:as\s+)?([a-z0-9_]+)', p)
                    if m2:
                        alias = m2.group(1)
        if tabla:
            res.append((tabla, alias))
    return res

def resolve_table_from_token(token: str, src_tables: list) -> str:
    """
    Dado un token tipo 't.id', 'schema.tabla.id', 'tabla.id' o 'tabla',
    devuelve el nombre completo 'schema.tabla' resolviendo alias si corresponde.
    Retorna None si no se puede determinar.
    """
    if not token or token == '*':
        return None
    # construir mapas de alias y de nombre base de tabla -> nombre completo
    alias_map = {alias: full for (full, alias) in src_tables if alias}
    base_map = {}
    for (full, alias) in src_tables:
        base = full.split('.')[-1]
        if base not in base_map:
            base_map[base] = full
    parts = token.split('.')
    if len(parts) >= 2:
        first, second = parts[0], parts[1]
        # si 'first' es alias conocido
        if first in alias_map:
            return alias_map[first]
        # si 'first.second' ya es un nombre completo de tabla
        cand = first + '.' + second
        for (full, _) in src_tables:
            if full == cand:
                return cand
        # si 'first' coincide con el nombre base de alguna tabla
        if first in base_map:
            return base_map[first]
    else:
        first = parts[0]
        if first in alias_map:
            return alias_map[first]
        if first in base_map:
            return base_map[first]
        # si es solo un nombre de columna y solo hay una tabla fuente, asumimos esa tabla
        fulls = [full for (full, _) in src_tables]
        uniques = []
        for f in fulls:
            if f not in uniques:
                uniques.append(f)
        if len(uniques) == 1:
            return uniques[0]
    # fallback global: si no se pudo resolver y hay una sola tabla fuente
    fulls = [full for (full, _) in src_tables]
    uniques = []
    for f in fulls:
        if f not in uniques:
            uniques.append(f)
    if len(uniques) == 1:
        return uniques[0]
    return None

def _fallback_tabla_origen(rec: dict, src_tables: list) -> None:
    """Si no se resolvió tabla_origen y hay una única tabla fuente, úsala."""
    try:
        if rec.get('tabla_origen') is None and src_tables:
            uniques = []
            for full, _alias in src_tables:
                if full not in uniques:
                    uniques.append(full)
            if len(uniques) == 1:
                rec['tabla_origen'] = uniques[0]
    except Exception:
        # fallback silencioso para no romper flujo
        pass


def _unique_origin_tokens(origin_cols: list) -> list:
    tokens = []
    for tok in origin_cols:
        if not tok:
            continue
        tok = tok.strip()
        if not tok or tok == '*':
            continue
        if tok not in tokens:
            tokens.append(tok)
    return tokens


def _is_function_without_fields(item: dict) -> bool:
    if item.get('origin_cols'):
        return False
    expr = (item.get('expr') or '').strip()
    raw = (item.get('raw') or '').strip()
    candidate = expr or raw
    if not candidate:
        return False
    candidate_lower = candidate.lower()
    if candidate_lower.startswith('case '):
        return False
    if '(' in candidate_lower and ')' in candidate_lower:
        return True
    if candidate_lower in {'current_timestamp', 'current_date', 'current_time', 'now'}:
        return True
    return False


def _build_records_for_item(item: dict, dest_col: str, target_table: str, src_tables: list,
                            stmt: str, recomendacion: str) -> list:
    records = []
    if item.get('is_star'):
        return records
    origins = _unique_origin_tokens(item.get('origin_cols', []))
    expr = (item.get('expr') or '').strip()
    if not expr:
        expr = (item.get('raw') or '').strip()
    is_copy = len(origins) == 1 and expr and expr == origins[0]
    transform = 'copy' if is_copy else (expr or None)

    if origins:
        for tok in origins:
            tabla = resolve_table_from_token(tok, src_tables)
            campo = tok.split('.')[-1] if '.' in tok else tok
            rec = {
                'id': str(uuid.uuid4()),
                'consulta': stmt,
                'tabla_origen': tabla,
                'tabla_destino': target_table,
                'campo_origen': campo,
                'campo_destino': dest_col,
                'transformacion_aplicada': transform,
                'recomendaciones': recomendacion
            }
            records.append(rec)
    elif _is_function_without_fields(item):
        expr_value = expr or item.get('raw') or 'funcion'
        rec = {
            'id': str(uuid.uuid4()),
            'consulta': stmt,
            'tabla_origen': 'funciones',
            'tabla_destino': target_table,
            'campo_origen': expr_value,
            'campo_destino': dest_col,
            'transformacion_aplicada': expr_value,
            'recomendaciones': recomendacion
        }
        records.append(rec)
    else:
        rec = {
            'id': str(uuid.uuid4()),
            'consulta': stmt,
            'tabla_origen': None,
            'tabla_destino': target_table,
            'campo_origen': None,
            'campo_destino': dest_col,
            'transformacion_aplicada': transform,
            'recomendaciones': recomendacion
        }
        records.append(rec)
    return records

def parse_ctes(stmt: str) -> dict:
    """
    Extrae definiciones de CTE a partir de una sentencia que comienza con WITH.
    Devuelve dict nombre_cte -> texto_select_de_cte
    """
    cte_map = {}
    stmt = stmt.strip()
    with_pos = find_top_level_keyword(stmt, 'with', 0)
    if with_pos == -1:
        return cte_map
    pos_insert = find_top_level_keyword(stmt, 'insert', with_pos)
    pos_create = find_top_level_keyword(stmt, 'create', with_pos)
    pos_select = find_top_level_keyword(stmt, 'select', with_pos)
    candidates = [p for p in [pos_insert, pos_create, pos_select] if p != -1]
    if candidates:
        main_pos = min(candidates)
        defs_str = stmt[with_pos + len('with'):main_pos].strip()
    else:
        defs_str = stmt[with_pos + len('with'):].strip()
    if not defs_str:
        return cte_map
    defs = top_level_split(defs_str, delimiter=',')
    for d in defs:
        d = d.strip()
        m = re.match(r'([a-z0-9_]+)\s+as\s*\((.*)\)$', d)
        if not m:
            continue
        name = m.group(1)
        body = m.group(2).strip()
        cte_map[name] = body
    return cte_map

def get_src_tables_with_ctes(from_clause: str, cte_map: dict) -> list:
    """
    Retorna tablas fuente incluyendo expansión de CTEs referenciados en el from_clause.
    """
    base_tables = extract_tables_from_from_clause(from_clause)
    expanded = []
    for table_name, alias in base_tables:
        expanded.extend(_expand_table_reference(table_name, alias, cte_map, set()))
    return expanded

def _expand_table_reference(table_name: str, alias: str, cte_map: dict, visited: set) -> list:
    """Expande CTEs recursivamente hasta llegar a tablas físicas."""
    if table_name in visited:
        return []
    if table_name in cte_map:
        visited.add(table_name)
        cte_sql = cte_map[table_name]
        inner_from = extract_from_clause(cte_sql)
        inner_tables = extract_tables_from_from_clause(inner_from)
        expanded = []
        for inner_table, inner_alias in inner_tables:
            propagated_alias = alias if alias else inner_alias
            res = _expand_table_reference(inner_table, propagated_alias, cte_map, visited)
            if res:
                expanded.extend(res)
            else:
                expanded.append((inner_table, propagated_alias))
        visited.remove(table_name)
        return expanded
    else:
        return [(table_name, alias)]

# -------------------------
# Parseo de target table y columnas (insert/create)
# -------------------------

def parse_insert_target(stmt: str):
    """
    Detecta target en sentencias insert ... into
    Retorna (tabla_destino, [lista_columnas] o None)
    """
    # buscar 'insert' top-level
    ins_pos = find_top_level_keyword(stmt, 'insert', 0)
    if ins_pos == -1:
        return None, None
    # buscar 'into' después de insert
    into_pos = find_top_level_keyword(stmt, 'into', ins_pos)
    # hay casos 'insert overwrite' -> handle: buscar 'overwrite' y luego 'into'
    if into_pos == -1:
        # tal vez 'insert overwrite table <table>' => buscamos 'table' o directamente schema.table
        # fallback: buscar primer nombre de tabla después de insert
        m = re.search(r'([a-z0-9_]+(?:\.[a-z0-9_]+)?)', stmt[ins_pos:])
        if m:
            tabla = m.group(1)
            # ver si hay lista de columnas entre paréntesis justo después
            after = stmt[ins_pos + m.end():]
            col_m = re.match(r'\s*\(\s*([^)]+)\)', after)
            if col_m:
                cols = [c.strip() for c in col_m.group(1).split(',')]
                return tabla, cols
            return tabla, None
        return None, None
    # a partir de into, saltar espacios y palabra 'table' si existe
    i = into_pos + len('into')
    rest = stmt[i:].lstrip()
    # si viene 'table' como palabra (impala a veces)
    if rest.startswith('table '):
        rest = rest[len('table '):].lstrip()
    # ahora tomar nombre de tabla (schema.table o simple)
    m = re.match(r'([a-z0-9_]+(?:\.[a-z0-9_]+)?)', rest)
    if not m:
        return None, None
    tabla = m.group(1)
    after = rest[m.end():].lstrip()
    # si next char es '(' => lista de columnas destino
    if after.startswith('('):
        # extraer contenido hasta ')'
        depth = 0
        cols = []
        cur = []
        j = 0
        while j < len(after):
            ch = after[j]
            if ch == '(':
                depth += 1
                if depth == 1:
                    j += 1
                    continue
            if ch == ')':
                depth -= 1
                if depth == 0:
                    break
            cur.append(ch)
            j += 1
        cols_str = ''.join(cur).strip()
        cols = [c.strip() for c in cols_str.split(',') if c.strip()]
        return tabla, cols
    return tabla, None

def parse_create_target(stmt: str):
    """
    Detecta target en create table ... as select
    Retorna (tabla_destino, [lista_columnas] o None, is_ctas_bool)
    """
    # buscar create top-level
    cr_pos = find_top_level_keyword(stmt, 'create', 0)
    if cr_pos == -1:
        return None, None, False
    # buscar 'table' top-level despuÃ©s de create
    tpos = find_top_level_keyword(stmt, 'table', cr_pos)
    if tpos == -1:
        return None, None, False
    # buscar nombre de tabla
    rest = stmt[tpos + len('table'):].lstrip()
    # soportar 'if not exists'
    if rest.startswith('if not exists'):
        rest = rest[len('if not exists'):].lstrip()
    m = re.match(r'([a-z0-9_]+(?:\.[a-z0-9_]+)?)', rest)
    if not m:
        return None, None, False
    tabla = m.group(1)
    after = rest[m.end():].lstrip()
    # si hay parÃ©ntesis con columnas explÃ­citas: create table t (c1, c2) as select ...
    if after.startswith('('):
        # extraer hasta ')'
        depth = 0; cur = []; j = 0
        while j < len(after):
            ch = after[j]
            if ch == '(':
                depth += 1
                if depth == 1:
                    j += 1; continue
            if ch == ')':
                depth -= 1
                if depth == 0:
                    break
            cur.append(ch)
            j += 1
        cols_str = ''.join(cur).strip()
        cols = [c.strip() for c in cols_str.split(',') if c.strip()]
    else:
        cols = None
    # determinar si es CTAS (as select)
    as_pos = find_top_level_keyword(stmt, 'as', tpos)
    select_pos = find_top_level_keyword(stmt, 'select', tpos)
    is_ctas = (as_pos != -1 and select_pos != -1 and as_pos < select_pos)
    return tabla, cols, is_ctas

def parse_create_like(stmt: str):
    """
    Detecta CREATE TABLE ... LIKE otra_tabla
    Retorna (tabla_destino, tabla_origen) o (None, None) si no matchea.
    """
    cr_pos = find_top_level_keyword(stmt, 'create', 0)
    if cr_pos == -1:
        return None, None
    tpos = find_top_level_keyword(stmt, 'table', cr_pos)
    if tpos == -1:
        return None, None
    rest = stmt[tpos + len('table'):].lstrip()
    if rest.startswith('if not exists'):
        rest = rest[len('if not exists'):].lstrip()
    m_dest = re.match(r'([a-z0-9_]+\.[a-z0-9_]+|[a-z0-9_]+)', rest)
    if not m_dest:
        return None, None
    dest = m_dest.group(1)
    after = rest[m_dest.end():].lstrip()
    like_pos = find_top_level_keyword(after, 'like', 0)
    if like_pos == -1:
        return None, None
    like_rest = after[like_pos + len('like'):].lstrip()
    m_src = re.match(r'([a-z0-9_]+\.[a-z0-9_]+|[a-z0-9_]+)', like_rest)
    if not m_src:
        return None, None
    src = m_src.group(1)
    return dest, src

# -------------------------
# Parse de items SELECT -> extraer origen de columna y alias
# -------------------------

def parse_select_item(item: str) -> dict:
    """
    Dado un item del select devuelve:
    {
      'raw': item,
      'origin_cols': [maybe one or more origen como 'schema.tbl.col' o 'col'],
      'alias': alias o None,
      'expr': expression (texto),
      'is_star': True/False
    }
    """
    res = {'raw': item, 'origin_cols': [], 'alias': None, 'expr': item.strip(), 'is_star': False}
    it = item.strip()
    # detectar aliases con ' as alias' o ' expr alias'
    # buscamos la presencia de ' as ' al top-level
    # simplificamos: si hay ' as ' la parte despuÃ©s es alias
    m_as = re.search(r'\s+as\s+([a-z0-9_]+)\s*$', it)
    if m_as:
        alias = m_as.group(1)
        expr = it[:m_as.start()].strip()
        res['alias'] = alias
        res['expr'] = expr
        it = expr
    else:
        # si no hay 'as', puede existir 'expr alias' -> detectamos Ãºltimo token simple al final
        m_alias2 = re.search(r'\s+([a-z0-9_]+)\s*$', it)
        if m_alias2:
            # para no confundir functions o 'case when', sÃ³lo tomamos alias si la parte antes no termina con un parÃ©ntesis ni contiene espacios raros
            before = it[:m_alias2.start()].strip()
            last_tok = m_alias2.group(1)
            # heurÃ­stica: si before contiene espacios y no termina en ')' o es una expresiÃ³n sencilla, consideramos alias
            if re.search(r'\s', before) and not before.endswith(')') and not before.endswith(']'):
                res['alias'] = last_tok
                res['expr'] = before
                it = before
    # detectar star
    if re.match(r'^\*$', it) or re.match(r'^[a-z0-9_]+\.\*$', it):
        res['is_star'] = True
        # si es table.* extraer la tabla
        if '.' in it:
            res['origin_cols'] = [it]  # e.g. sbani.tabla.*
        else:
            res['origin_cols'] = ['*']
        return res
    # extraer columnas simples de la expresiÃ³n: buscar patrones schema.tab.col o table.col o bare col
    # bÃºsqueda de formatos schema.table.col o table.col o col
    # buscar todos los identificadores separados por punto
    col_refs = re.findall(r'([a-z0-9_]+\.[a-z0-9_]+\.[a-z0-9_]+|[a-z0-9_]+\.[a-z0-9_]+|[a-z0-9_]+)', it)
    # col_refs incluye tokens y palabras; no todos son columnas; filtramos palabras reservadas y functions comunes
    keywords = set(['case','when','then','else','end','count','sum','min','max','avg','cast','over','partition','order','by','desc','asc','distinct','row_number','and','or','coalesce','lag','lead','dense_rank','rank','current_timestamp','current_date','current_time','now'])
    cols = []
    for token in col_refs:
        if token in keywords:
            continue
        # token que tiene punto puede ser table.col o schema.table (si tiene dos puntos lo dejamos)
        # heurÃ­stica: si token coincide con funcname(...) no lo incluimos (pero el regex ya saca solo nombres)
        cols.append(token)
    # preferimos detectar referencias columna tipo table.col o schema.table.col
    res['origin_cols'] = cols
    return res

# -------------------------
# Construir mapeos de linaje por sentencia
# -------------------------

def lineage_from_statement(stmt: str, cte_map: dict=None) -> list:
    """
    Dada una sentencia SQL (normalizada en lowercase), retorna lista de registros de linaje (dicts).
    """
    stmt = stmt.strip()
    cte_map = cte_map or {}
    results = []

    # si comienza con WITH, extraer CTEs y procesar la sentencia principal
    if stmt.startswith('with '):
        cte_map_local = parse_ctes(stmt)
        cte_combined = dict(cte_map)
        cte_combined.update(cte_map_local)
        pos_insert = find_top_level_keyword(stmt, 'insert', 0)
        pos_create = find_top_level_keyword(stmt, 'create', 0)
        pos_select = find_top_level_keyword(stmt, 'select', 0)
        candidates = [p for p in [pos_insert, pos_create, pos_select] if p and p > 0]
        if candidates:
            main_pos = min(candidates)
            main_stmt = stmt[main_pos:]
            return lineage_from_statement(main_stmt, cte_combined)
        rec = {
            'id': str(uuid.uuid4()),
            'consulta': stmt,
            'tabla_origen': None,
            'tabla_destino': None,
            'campo_origen': None,
            'campo_destino': None,
            'transformacion_aplicada': None,
            'recomendaciones': 'with detectado pero no se pudo localizar la sentencia principal (insert/create/select) - revisar manualmente'
        }
        return [rec]

    # primero detectar si es create table ... like
    dest_like, src_like = parse_create_like(stmt)
    if dest_like and src_like:
        rec = {
            'id': str(uuid.uuid4()),
            'consulta': stmt,
            'tabla_origen': src_like,
            'tabla_destino': dest_like,
            'campo_origen': None,
            'campo_destino': None,
            'transformacion_aplicada': None,
            'recomendaciones': 'create table like: relacion a nivel de tablas'
        }
        return [rec]

    # luego detectar si es create table ... as select (ctas)
    tabla_create, cols_create, is_ctas = parse_create_target(stmt)
    if is_ctas and tabla_create:
        # CTAS: tabla destino = tabla_create
        target_table = tabla_create
        target_cols = cols_create  # puede ser None
        select_items = extract_select_items(stmt)
        parsed_items = [parse_select_item(it) for it in select_items]
        from_clause = extract_from_clause(stmt)
        # combinar CTEs locales (dentro del CTAS) con el mapa heredado
        cte_local = parse_ctes(stmt)
        cte_merged = dict(cte_map)
        cte_merged.update(cte_local)
        src_tables = get_src_tables_with_ctes(from_clause, cte_merged)
        # si select_items contiene alguna star o no se especifican columnas destino -> relaciÃ³n tabla->tabla
        has_star = any(item['is_star'] for item in parsed_items)
        if has_star or target_cols is None and (len(select_items) == 0 or any(item.strip() == '' for item in select_items)):
            # relaciÃ³n a nivel tabla: por las reglas del usuario, si se crea tabla en base al esquema de otra y no se indican campos -> tabla->tabla
            for (src_tab, alias) in src_tables:
                rec = {
                    'id': str(uuid.uuid4()),
                    'consulta': stmt,
                    'tabla_origen': src_tab,
                    'tabla_destino': target_table,
                    'campo_origen': None,
                    'campo_destino': None,
                    'transformacion_aplicada': None,
                    'recomendaciones': 'relacion a nivel de tablas (ctas sin lista de campos o uso de *) - verificar esquema en metastore si necesita mapping columna a columna'
                }
                _fallback_tabla_origen(rec, src_tables)
                results.append(rec)
            # si no hay src_tables detectadas, crear un registro general
            if not src_tables:
                rec = {
                    'id': str(uuid.uuid4()),
                    'consulta': stmt,
                    'tabla_origen': None,
                    'tabla_destino': target_table,
                    'campo_origen': None,
                    'campo_destino': None,
                    'transformacion_aplicada': None,
                    'recomendaciones': 'relacion a nivel de tablas (ctas sin lista de campos) - no se detectaron tablas origen'
                }
                _fallback_tabla_origen(rec, src_tables)
                results.append(rec)
        else:
            # mapeo columna a columna (intentar inferir)
            if target_cols:
                for idx, item in enumerate(parsed_items):
                    dest_col = target_cols[idx] if idx < len(target_cols) else None
                    if dest_col is None:
                        if item.get('alias'):
                            dest_col = item['alias']
                        elif item.get('origin_cols'):
                            tmp = item['origin_cols'][-1]
                            dest_col = tmp.split('.')[-1] if '.' in tmp else tmp
                    records = _build_records_for_item(
                        item,
                        dest_col,
                        target_table,
                        src_tables,
                        stmt,
                        'verificar expresiones y tipos; mapping inferido por posicion en ctas con columnas destino'
                    )
                    for rec in records:
                        if rec['tabla_origen'] != 'funciones':
                            _fallback_tabla_origen(rec, src_tables)
                        results.append(rec)
            else:
                for item in parsed_items:
                    dest_col = item['alias'] if item['alias'] else (item['origin_cols'][-1] if item['origin_cols'] else None)
                    if dest_col and '.' in dest_col:
                        dest_col = dest_col.split('.')[-1]
                    records = _build_records_for_item(
                        item,
                        dest_col,
                        target_table,
                        src_tables,
                        stmt,
                        'mapping inferido sin lista destino; se recomienda especificar columnas en create table (...) as select (...) para mayor precision'
                    )
                    for rec in records:
                        if rec['tabla_origen'] != 'funciones':
                            _fallback_tabla_origen(rec, src_tables)
                        results.append(rec)
            return results

    # -------------------------
    # Caso INSERT ... SELECT
    # -------------------------
    tabla_insert, cols_insert = parse_insert_target(stmt)
    if tabla_insert:
        target_table = tabla_insert
        target_cols = cols_insert  # None o lista
        select_items = extract_select_items(stmt)
        from_clause = extract_from_clause(stmt)
        src_tables = get_src_tables_with_ctes(from_clause, cte_map)
        # parse items
        parsed_items = [parse_select_item(it) for it in select_items]
        # si existe algÃºn item is_star => relaciÃ³n tabla->tabla
        any_star = any(it['is_star'] for it in parsed_items)
        if any_star:
            # cuando hay '*' en el select sin conocer campos, mantÃ©n relaciÃ³n a nivel de tablas
            for (src_tab, alias) in src_tables:
                rec = {
                    'id': str(uuid.uuid4()),
                    'consulta': stmt,
                    'tabla_origen': src_tab,
                    'tabla_destino': target_table,
                    'campo_origen': None,
                    'campo_destino': None,
                    'transformacion_aplicada': None,
                    'recomendaciones': 'relacion a nivel de tablas por uso de * en el select; si necesita mapping columna a columna, especificar columnas en el insert o consultar metastore'
                }
                _fallback_tabla_origen(rec, src_tables)
                results.append(rec)
            if not src_tables:
                rec = {
                    'id': str(uuid.uuid4()),
                    'consulta': stmt,
                    'tabla_origen': None,
                    'tabla_destino': target_table,
                    'campo_origen': None,
                    'campo_destino': None,
                    'transformacion_aplicada': None,
                    'recomendaciones': 'relacion a nivel de tablas por uso de *; no se detectaron tablas origen'
                }
                _fallback_tabla_origen(rec, src_tables)
                results.append(rec)
            return results
        # No hay stars -> intentamos mapear columnas
        if target_cols:
            for idx, item in enumerate(parsed_items):
                dest_col = target_cols[idx] if idx < len(target_cols) else None
                if dest_col is None:
                    if item.get("alias"):
                        dest_col = item["alias"]
                    elif item.get("origin_cols"):
                        tmp = item["origin_cols"][-1]
                        dest_col = tmp.split(".")[-1] if "." in tmp else tmp
                records = _build_records_for_item(
                    item,
                    dest_col,
                    target_table,
                    src_tables,
                    stmt,
                    'mapping por posicion entre select y lista de columnas destino'
                )
                for rec in records:
                    if rec["tabla_origen"] != 'funciones':
                        _fallback_tabla_origen(rec, src_tables)
                    results.append(rec)
        else:
            for item in parsed_items:
                dest_col = item["alias"] if item["alias"] else (item["origin_cols"][-1] if item["origin_cols"] else None)
                if dest_col and '.' in dest_col:
                    dest_col = dest_col.split(".")[-1]
                records = _build_records_for_item(
                    item,
                    dest_col,
                    target_table,
                    src_tables,
                    stmt,
                    'mapping inferido sin lista destino; se recomienda especificar columnas en el insert para mayor claridad'
                )
                for rec in records:
                    if rec["tabla_origen"] != 'funciones':
                        _fallback_tabla_origen(rec, src_tables)
                    results.append(rec)
        return results

    # -------------------------
# -------------------------
# FunciÃ³n principal pÃºblica
# -------------------------

def generar_linaje_impala(sql_text: str) -> list:
    """
    Dado un texto sql (puede contener mÃºltiples sentencias) devuelve lista de registros de linaje.
    """
    sql_norm = normalize_sql(sql_text)
    stmts = split_statements_top_level(sql_norm)
    all_results = []
    for s in stmts:
        if not s.strip():
            continue
        recs = lineage_from_statement(s)
        # garantizar que todas las claves estÃ©n en minÃºscula y sin None problemÃ¡tico (dejamos None para campos vacÃ­os)
        for r in recs:
            # normalizar strings a minÃºsculas (si existen)
            for k in ['consulta','tabla_origen','tabla_destino','campo_origen','campo_destino','transformacion_aplicada','recomendaciones']:
                if k in r and isinstance(r[k], str):
                    r[k] = r[k].lower()
            all_results.append(r)
    return all_results

def guardar_linaje_en_json(datos, ruta='json/linaje.json'):
    os.makedirs('json', exist_ok=True)
    with open(ruta, 'w', encoding='utf-8') as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)

# -------------------------
# Ejemplos de uso / pruebas
# -------------------------

if __name__ == "__main__":
    ejemplos = [
        # insert con columnas explicitas
        """
        insert into sbani.dest_table (id, nombre, telefono)
        select t.id, t.full_name as nombre, t.phone from sbani.source_table t;
        """,
        # insert con star
        """
        insert into sbani.dest_all
        select * from sbani.source_all;
        """,
        # create table as select (ctas) sin columnas
        """
        create table sbani.ctas_table as
        select a.col1, b.col2 from sbani.tabla_a a join sbani.tabla_b b on a.id = b.id;
        """,
        # with ... insert ... as
        """
        with cte as (
           select id, valor from sbani.origen
        )
        insert into sbani.destino select id, valor from cte;
        """,
        # create like (estructura) y luego insert con filtro
        """
        CREATE TABLE IF NOT EXISTS dwh.ventas_estructura LIKE raw.ventas;

        INSERT INTO dwh.ventas_estructura
        SELECT * FROM raw.ventas
        WHERE fecha_venta >= '2025-01-01';
        """,
        # CTAS con WITH y múltiples CTEs y joins + agregaciones
        """
        CREATE TABLE IF NOT EXISTS dwh.ventas_resumen
        STORED AS PARQUET
        AS
        WITH ventas_filtradas AS (
            SELECT 
                v.cliente_id,
                v.producto_id,
                v.cantidad,
                v.precio,
                v.fecha_venta
            FROM raw.ventas v
            WHERE v.fecha_venta >= '2025-01-01'
        ),
        clientes_activos AS (
            SELECT
                c.cliente_id,
                c.nombre,
                c.segmento
            FROM mkt.clientes c
            WHERE c.estado = 'activo'
        )
        SELECT
            ca.cliente_id,
            ca.nombre,
            ca.segmento,
            SUM(vf.cantidad * vf.precio) AS total_comprado,
            COUNT(DISTINCT vf.producto_id) AS productos_distintos
        FROM ventas_filtradas vf
        JOIN clientes_activos ca
            ON vf.cliente_id = ca.cliente_id
        GROUP BY ca.cliente_id, ca.nombre, ca.segmento;
        """,
        # CTAS similar sin esquema en nombres
        """
        CREATE TABLE IF NOT EXISTS ventas_resumen
        STORED AS PARQUET
        AS
        WITH ventas_filtradas AS (
            SELECT 
                cliente_id,
                producto_id,
                cantidad,
                precio,
                fecha_venta
            FROM ventas
            WHERE fecha_venta >= '2025-01-01'
        ),
        totales_por_cliente AS (
            SELECT
                cliente_id,
                SUM(cantidad * precio) AS total_comprado,
                COUNT(DISTINCT producto_id) AS productos_distintos
            FROM ventas_filtradas
            GROUP BY cliente_id
        )
        SELECT
            c.cliente_id,
            c.total_comprado,
            c.productos_distintos,
            CURRENT_TIMESTAMP() AS fecha_proceso
        FROM totales_por_cliente c;
        """
    ]

    linajes = []
    for idx, sql in enumerate(ejemplos, 1):
        print("SQL ----")
        print(sql.strip())
        lin = generar_linaje_impala(sql)
        linajes.extend(lin)
        print("Lineage JSON:")
        print(json.dumps(lin, indent=2, ensure_ascii=False))
        print("\n" + "="*60 + "\n")
    # Guardar resultado en archivo JSON
    df_linaje = pd.DataFrame(linajes)
    print("DataFrame generado (primeras filas):")
    
    # print(df_linaje.head())
    #guardar_linaje_en_json(linajes)
