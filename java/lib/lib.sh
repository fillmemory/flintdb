#!/usr/bin/env bash
#
# Install WebUI, Parquet optional dependencies
# Equivalent "gradle copyDependencies"
#

set -euo pipefail

# Download (optional) libraries from Maven Central into lib/gradle-deps.
# Keeps versions aligned with the jars already vendored in gradle-deps.
# You can adjust LIBS below to add more coordinates (groupId:artifactId:version).
#

MAVEN_REPO_URL="https://repo1.maven.org/maven2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${SCRIPT_DIR}/optional"
mkdir -p "${TARGET_DIR}"

# Core / frequently used libraries present in gradle-deps (add/remove as needed).
# Format: groupId:artifactId:version
LIBS=()

# Parse command line arguments
for arg in "$@"; do
    case "$arg" in
        --with-webui) WITH_WEBUI=1 ;;
        --with-sqlite) WITH_SQLITE=1 ;;
        --with-parquet) WITH_PARQUET=1 ;;
    esac
done

# check commandline arguments if --with-gson
if [[ "${WITH_WEBUI:-0}" == 1 ]]; then
    LIBS+=("com.google.code.gson:gson:2.9.0")
fi
if [[ "${WITH_SQLITE:-0}" == 1 ]]; then
    LIBS+=("org.xerial:sqlite-jdbc:3.51.0.0")
fi
# check commandline arguments if --with-parquet
if [[ "${WITH_PARQUET:-0}" == 1 ]]; then
    LIBS+=(
    "org.apache.commons:commons-compress:1.27.1"
    "org.apache.commons:commons-lang3:3.12.0"
    "com.github.luben:zstd-jni:1.5.6-5"   
    "org.apache.parquet:parquet-avro:1.15.2"
    # Use shaded parquet-hadoop-bundle to avoid Hadoop runtime deps
    "org.apache.parquet:parquet-hadoop-bundle:1.15.2"
    "org.apache.hadoop:hadoop-common:3.4.1"
    "org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1"
    "org.apache.hadoop:hadoop-mapreduce-client-common:3.4.1"
    "org.apache.hadoop:hadoop-mapreduce-client-app:3.4.1"

    # Snappy runtime (optional but common)
    "org.xerial.snappy:snappy-java:1.1.10.5"
    )
fi

# print usage if LIBS is empty
if [[ ${#LIBS[@]} -eq 0 ]]; then
    echo "No libraries specified. Use --with-webui or --with-parquet to include dependencies."
    echo "Usage: ${BASH_SOURCE[0]} --with-webui --with-parquet"
    exit 1
fi

# Enable to recursively resolve transitive dependencies via POM parsing
# export RESOLVE_TRANSITIVE=1 (or pass inline: RESOLVE_TRANSITIVE=1 ./lib.sh)
RESOLVE_TRANSITIVE="${RESOLVE_TRANSITIVE:-1}"

# Additional resolution flags
# INCLUDE_PROVIDED=1 to also fetch provided scope deps
# INCLUDE_OPTIONAL=1 to include optional=true deps
# RECURSIVE_PARENTS_DEPTH=N (default 1) to walk up N parent levels gathering properties + dependencyManagement
# DEBUG_POM=1 for verbose include/skip reasons
INCLUDE_PROVIDED="${INCLUDE_PROVIDED:-0}"
INCLUDE_OPTIONAL="${INCLUDE_OPTIONAL:-0}"
RECURSIVE_PARENTS_DEPTH="${RECURSIVE_PARENTS_DEPTH:-2}"  # allow deeper walk; set 0 for unlimited
DEBUG_POM="${DEBUG_POM:-0}"

# DEDUPE_ARTIFACTS=1 prevents downloading multiple versions of the same groupId:artifactId.
# Strategy:
#   DEDUPE_STRATEGY=first  (default) keep the first encountered version, skip later ones.
#   DEDUPE_STRATEGY=highest choose the highest semantic-ish version; if a higher version appears later
#                       it replaces the earlier one (removes old jar and downloads the higher one).
DEDUP_ARTIFACTS="${DEDUP_ARTIFACTS:-1}"
DEDUP_STRATEGY="${DEDUP_STRATEGY:-first}"  # first | highest

TMP_DIR="${SCRIPT_DIR}/.tmp-lib-resolve"
mkdir -p "$TMP_DIR"

# ----------------------------------------------------------------------------
# Pre-download exclusion filtering
# ----------------------------------------------------------------------------
# Instead of downloading jars only to delete them later (see prior rm section),
# we allow skipping certain artifacts up-front based on shell globs that match
# their artifactId (or resulting jar filename). This saves time + bandwidth.
#
# Configure via environment:
#   DISABLE_EXCLUDES=1        -> turn feature off
#   APPEND_EXCLUDES="foo* bar" -> space separated extra patterns to append
#   OVERRIDE_EXCLUDES="pattern1 pattern2" -> replace defaults entirely
#
# The defaults replicate the formerly deleted jars list. Review to ensure they
# don't hide dependencies you actually need. Remove entries or set
# DISABLE_EXCLUDES=1 to bypass.
# ----------------------------------------------------------------------------
if [[ "${DISABLE_EXCLUDES:-0}" != 1 ]]; then
    if [[ -n "${OVERRIDE_EXCLUDES:-}" ]]; then
        # User supplied full override list
        IFS=' ' read -r -a EXCLUDE_GLOBS <<<"${OVERRIDE_EXCLUDES}"
    else
        # Default patterns (artifactId globs)
        EXCLUDE_GLOBS=(
            aircompressor* aopalliance* audience* assertj* awaitility* bcprov* byte-buddy* checker* \
            commons-bean* commons-cli* commons-codec* commons-io* commons-math3* commons-net* commons-pool* commons-text* \
            curator* dnsjava* error_prone_annotations* failureaccess* guava* guice* hamcrest* \
            hadoop-mapreduce-client-jobclient* hadoop-shaded-protobuf* hadoop-yarn* http* \
            jakarta.* javassist* javax.* jaxb* j2objc* jcip* jersey* jetty* jettison* \
            jsch* jsp* jsr* junit* kerb* logback* metrics* mockito* netty* nimbus* objenesis* re2j* reflections* zookeeper*
        )
        if [[ -n "${APPEND_EXCLUDES:-}" ]]; then
            while IFS=' ' read -r tok; do [[ -n "$tok" ]] && EXCLUDE_GLOBS+=("$tok"); done <<<"${APPEND_EXCLUDES}"
        fi
    fi
else
    EXCLUDE_GLOBS=()
fi

is_excluded_artifact() {
    local aid="$1"; local g; [[ ${#EXCLUDE_GLOBS[@]} -eq 0 ]] && return 1
    for g in "${EXCLUDE_GLOBS[@]}"; do
        if [[ "$aid" == $g ]]; then return 0; fi
        # Also allow pattern to match full jar filename prefix/suffix logic implicitly via glob
    done
    return 1
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }

download() {
    local url="$1" dest="$2"
    if have_cmd wget; then
        wget -q -O "$dest" "$url"
    elif have_cmd curl; then
        curl -fsSL -o "$dest" "$url"
    else
        echo "Error: neither wget nor curl found in PATH" >&2
        return 1
    fi
}

if [[ "$RESOLVE_TRANSITIVE" == 1 ]]; then
    _t_state="enabled"
else
    _t_state="disabled"
fi
echo "Root artifacts listed: ${#LIBS[@]} (transitives ${_t_state}; provided=${INCLUDE_PROVIDED}; optional=${INCLUDE_OPTIONAL}; parentDepth=${RECURSIVE_PARENTS_DEPTH})" >&2

declare -a queue
queue=("${LIBS[@]}")
processed_file="${TMP_DIR}/processed.txt"
touch "$processed_file"
if [[ "${LIB_RESUME:-0}" != 1 ]]; then
    : >"$processed_file"  # truncate for a fresh run
fi

is_processed() { grep -Fqx "$1" "$processed_file" 2>/dev/null; }
mark_processed() { echo "$1" >>"$processed_file"; }

version_gt() {
    # returns 0 (true) if $1 > $2
    # simplistic comparator: split on non-alphanum, compare numeric parts numerically, others lexicographically
    local IFS='.'
    local a b i max
    # replace non-alphanumerics with dots then split
    a=$(echo "$1" | sed 's/[^0-9A-Za-z]/./g')
    b=$(echo "$2" | sed 's/[^0-9A-Za-z]/./g')
    IFS='.' read -r -a A <<<"$a"
    IFS='.' read -r -a B <<<"$b"
    (( ${#A[@]} > ${#B[@]} )) && max=${#A[@]} || max=${#B[@]}
    for ((i=0;i<max;i++)); do
        av="${A[i]:-0}"; bv="${B[i]:-0}"
        if [[ $av =~ ^[0-9]+$ && $bv =~ ^[0-9]+$ ]]; then
            if ((10#$av > 10#$bv)); then return 0; elif ((10#$av < 10#$bv)); then return 1; fi
        else
            if [[ "$av" > "$bv" ]]; then return 0; elif [[ "$av" < "$bv" ]]; then return 1; fi
        fi
    done
    return 1  # equal or not greater
}

# Portable (bash 3.2 compatible) dedupe map stored in a text file: key<space>version
DEDUP_MAP_FILE="${TMP_DIR}/dedup_map.txt"
touch "$DEDUP_MAP_FILE"

get_dedup_version() { awk -v k="$1" '$1==k {print $2; exit}' "$DEDUP_MAP_FILE" 2>/dev/null || true; }
set_dedup_version() {
    local key="$1" ver="$2"
    # rewrite file without old key then append new line
    if [[ -s "$DEDUP_MAP_FILE" ]]; then
        awk -v k="$key" '$1!=k' "$DEDUP_MAP_FILE" >"${DEDUP_MAP_FILE}.tmp" || true
        mv "${DEDUP_MAP_FILE}.tmp" "$DEDUP_MAP_FILE"
    fi
    echo "$key $ver" >>"$DEDUP_MAP_FILE"
}

fetch_pom_and_list_dependencies() {
    local coord="$1"; IFS=':' read -r G A V <<<"$coord"
    local group_path=${G//./\/}
    local pom_url="${MAVEN_REPO_URL}/${group_path}/${A}/${V}/${A}-${V}.pom"
    local pom_file="${TMP_DIR}/${A}-${V}.pom"
    if [[ ! -f "$pom_file" ]]; then
        if ! download "$pom_url" "$pom_file"; then
            echo "WARN: could not fetch POM $pom_url" >&2
            return 0
        fi
    fi
    # Use python (if available) for robust XML parsing.
    if command -v python3 >/dev/null 2>&1; then
        python3 - "$pom_file" "$INCLUDE_PROVIDED" "$INCLUDE_OPTIONAL" "$RECURSIVE_PARENTS_DEPTH" "$DEBUG_POM" "$V" <<'PY' || true
import sys, re, os, xml.etree.ElementTree as ET, urllib.request
from pathlib import Path

pom_path = Path(sys.argv[1])
INCLUDE_PROVIDED = sys.argv[2] == '1'
INCLUDE_OPTIONAL = sys.argv[3] == '1'
PARENT_DEPTH_RAW = sys.argv[4] if len(sys.argv) > 4 else '1'
try:
    PARENT_DEPTH = int(PARENT_DEPTH_RAW)
except ValueError:
    PARENT_DEPTH = 1
# PARENT_DEPTH <= 0 means unlimited
DEBUG = (len(sys.argv) > 5 and sys.argv[5] == '1')
ROOT_VERSION = sys.argv[6] if len(sys.argv) > 6 else ''

MAVEN_BASE = "https://repo1.maven.org/maven2"

def dlog(msg):
    if DEBUG:
        print(f"[POM] {msg}", file=sys.stderr)

def read_root(path: Path):
    try:
        return ET.parse(path).getroot()
    except Exception as e:
        dlog(f"Failed parse {path.name}: {e}")
        return None

root = read_root(pom_path)
if root is None:
    sys.exit(0)

if root.tag.startswith('{'):
    ns = root.tag.split('}',1)[0][1:]
else:
    ns = ''
def n(tag):
    return f"{{{ns}}}{tag}" if ns else tag

props = {}
dependency_mgmt = {}

def collect_properties(r):
    p = r.find(n('properties'))
    if p is not None:
        for c in p:
            if c.text:
                key = c.tag.split('}',1)[-1]
                val = c.text.strip()
                # Overwrite rule: if key missing OR existing value still unresolved placeholder and new one is more concrete
                if (key not in props) or ('${' in props[key] and ('${' not in val or val != props[key])):
                    props[key] = val

def parse_dependency_mgmt(r):
    dm = r.find(n('dependencyManagement'))
    if dm is not None:
        deps = dm.find(n('dependencies'))
        if deps is not None:
            for d in deps.findall(n('dependency')):
                gid = resolve(d.findtext(n('groupId')) or '')
                aid = resolve(d.findtext(n('artifactId')) or '')
                ver = resolve(d.findtext(n('version')) or '')
                if gid and aid and ver:
                    # child overrides parent; only add if absent
                    if (gid, aid) not in dependency_mgmt:
                        dependency_mgmt[(gid, aid)] = ver

def resolve(text):
    if not text:
        return text
    pattern = re.compile(r'\$\{([^}]+)\}')
    def repl(m):
        return props.get(m.group(1), m.group(0))
    # iterate until stable (guard cycles)
    prev = None
    cur = text
    for _ in range(5):
        if cur == prev: break
        prev = cur
        cur = pattern.sub(repl, cur)
    return cur

# Gather child first
def capture_project_version(r):
    # Maven implicit property project.version
    ver = r.findtext(n('version')) or ''
    if ver:
        # always override so that when traversing parents the current scope's project.version is correct
        if 'project.version' not in props:
            props['project.version'] = ver.strip()
    gid = r.findtext(n('groupId')) or ''
    if gid and 'project.groupId' not in props:
        props['project.groupId'] = gid.strip()

capture_project_version(root)
collect_properties(root)
parse_dependency_mgmt(root)
if 'hadoop.version' not in props and 'project.version' in props:
    props['hadoop.version'] = props['project.version']

def fetch_parent(gid, aid, ver, level):
    group_path = gid.replace('.', '/')
    url = f"{MAVEN_BASE}/{group_path}/{aid}/{ver}/{aid}-{ver}.pom"
    fname = pom_path.parent / f".{aid}-{ver}.parent{level}.pom"
    try:
        data = urllib.request.urlopen(url, timeout=8).read()
        fname.write_bytes(data)
        dlog(f"Fetched parent {gid}:{aid}:{ver} (level {level})")
        return read_root(fname)
    except Exception as e:
        dlog(f"Parent fetch failed {url}: {e}")
        return None

level = 0
current = root
while (PARENT_DEPTH <= 0) or (level < PARENT_DEPTH):
    parent = current.find(n('parent'))
    if parent is None:
        break
    pgid = resolve(parent.findtext(n('groupId')) or '')
    paid = resolve(parent.findtext(n('artifactId')) or '')
    pver = resolve(parent.findtext(n('version')) or '')
    if not (pgid and paid and pver):
        break
    level += 1
    p_root = fetch_parent(pgid, paid, pver, level)
    if p_root is None:
        break
    # parent properties should fill only missing keys -> pass p_root to collectors AFTER child; do not overwrite child's project.version
    collect_properties(p_root)
    parse_dependency_mgmt(p_root)
    current = p_root

# After parent traversal, ensure hadoop.version captured if present in any loaded parents
if 'hadoop.version' not in props:
    # brute force scan of loaded parent pom files in same directory
    try:
        from pathlib import Path as _P
        for _pf in pom_path.parent.glob('.*.parent*.pom'):
            try:
                txt = _pf.read_text(encoding='utf-8', errors='ignore')
                import re as _re
                m = _re.search(r'<hadoop.version>([^<]+)</hadoop.version>', txt)
                if m:
                    props['hadoop.version'] = m.group(1).strip()
                    break
            except Exception:
                pass
    except Exception:
        pass
    # final fallback: mirror project.version if still absent
    if 'hadoop.version' not in props and 'project.version' in props:
        props['hadoop.version'] = props['project.version']

# Second pass: resolve any property values that reference other properties
for _ in range(4):
    changed = False
    for k,v in list(props.items()):
        if '${' in v:
            nv = resolve(v)
            if nv != v:
                props[k] = nv
                changed = True
    if not changed:
        break

# Re-resolve dependencyManagement versions now that more props may exist
for (g,a),v in list(dependency_mgmt.items()):
    if '${' in v:
        nv = resolve(v)
        dependency_mgmt[(g,a)] = nv

deps_parent = root.find(n('dependencies'))
if deps_parent is None:
    sys.exit(0)

accepted_scopes = {'', 'compile', 'runtime'}

# Heuristic: if hadoop.version missing but project.version present, mirror it.
if 'hadoop.version' not in props and 'project.version' in props:
    props['hadoop.version'] = props['project.version']

for dep in deps_parent.findall(n('dependency')):
    gid_raw = dep.findtext(n('groupId')) or ''
    aid_raw = dep.findtext(n('artifactId')) or ''
    ver_raw = dep.findtext(n('version')) or ''
    scope = (dep.findtext(n('scope')) or '').strip()
    optional_flag = (dep.findtext(n('optional')) or '').strip().lower() == 'true'
    gid = resolve(gid_raw)
    aid = resolve(aid_raw)
    ver = resolve(ver_raw)
    # If still unresolved and looks like ${hadoop.version}, attempt heuristic: use collected hadoop.version or project.version
    if ver and '${' in ver:
        placeholder = ver.strip()
        if 'project.groupId' in props and gid == '${project.groupId}':
            gid = props['project.groupId']
        if 'hadoop.version' in placeholder:
            hv = props.get('hadoop.version') or os.environ.get('HADOOP_VERSION')
            pv = props.get('project.version')
            if hv and '${' not in hv:
                ver = hv
            elif pv and '${' not in pv:
                ver = pv
            elif ROOT_VERSION:
                ver = ROOT_VERSION
        # generic fallback: if still unresolved and equals ${project.version}
        if ver and '${' in ver and ver.strip() == '${project.version}':
            pv = props.get('project.version')
            if pv and '${' not in pv:
                ver = pv
    if not ver:
        ver = dependency_mgmt.get((gid, aid), '')
    reason = []
    include = True
    if scope == 'test':
        include = False; reason.append('scope=test')
    elif scope == 'provided' and not INCLUDE_PROVIDED:
        include = False; reason.append('scope=provided filtered')
    elif scope not in accepted_scopes and scope not in ('provided', 'system'):
        # other exotic scopes -> skip
        include = False; reason.append(f'scope={scope} unknown')
    if optional_flag and not INCLUDE_OPTIONAL:
        include = False; reason.append('optional')
    if not gid or not aid:
        include = False; reason.append('missing gid/aid')
    if include and not ver:
        include = False; reason.append('missing version')
    if include:
        if DEBUG:
            reason.append(f'scope={scope or "compile"}')
            # flag unresolved placeholders to help debugging
            if ver and '${' in ver:
                reason.append('UNRESOLVED_VERSION')
            print(f"[POM] include {gid}:{aid}:{ver} ({', '.join(reason)})", file=sys.stderr)
        if ver and '${' in ver:
            # skip adding unresolved placeholder dependency to avoid 404 fetch attempts
            if DEBUG:
                print(f"[POM] skip {gid}:{aid}:{ver} -> unresolved placeholder", file=sys.stderr)
        else:
            print(f"{gid}:{aid}:{ver}")
    else:
        if DEBUG:
            print(f"[POM] skip {gid_raw}:{aid_raw}:{ver_raw} -> {'; '.join(reason)}", file=sys.stderr)
PY
    else
        # Fallback: very naive grep (may over/under include)
        awk -v inc_prov="$INCLUDE_PROVIDED" -v inc_opt="$INCLUDE_OPTIONAL" 'BEGIN{RS="<dependency>"} NR>1 {
            testScope=match($0,/<scope>test<\/scope>/)
            provScope=match($0,/<scope>provided<\/scope>/)
            opt=match($0,/<optional>true<\/optional>/)
            if (testScope) next
            if (provScope && inc_prov != 1) next
            if (opt && inc_opt != 1) next
            if ($0 ~ /<groupId>/ && $0 ~ /<artifactId>/ && $0 ~ /<version>/) {
                gsub(/.*<groupId>/,""); gsub(/<\/groupId>.*/,"&"); gid=$0; sub(/<\/groupId>.*/,"",gid)
                gsub(/.*<artifactId>/,""); gsub(/<\/artifactId>.*/,"&"); aid=$0; sub(/<\/artifactId>.*/,"",aid)
                gsub(/.*<version>/,""); gsub(/<\/version>.*/,"&"); ver=$0; sub(/<\/version>.*/,"",ver)
                print gid":"aid":"ver
            }
        }' "$pom_file"
    fi
}

while true; do
    # Safely check queue length even with set -u
    qlen=${#queue[@]:-0}
    (( qlen == 0 )) && break
    coord="${queue[0]}"; queue=("${queue[@]:1}")
    if is_processed "$coord"; then
        continue
    fi
    IFS=':' read -r GROUP_ID ARTIFACT_ID VERSION <<<"$coord"
    if [[ -z "$GROUP_ID" || -z "$ARTIFACT_ID" || -z "$VERSION" ]]; then
        echo "Skipping invalid coordinate: $coord" >&2
        mark_processed "$coord"
        continue
    fi

    if [[ "$DEDUP_ARTIFACTS" == 1 ]]; then
        dedup_key="${GROUP_ID}:${ARTIFACT_ID}"
        chosen_ver="$(get_dedup_version "$dedup_key")"
        if [[ -n "$chosen_ver" ]]; then
            case "$DEDUP_STRATEGY" in
                first)
                    if [[ "$VERSION" != "$chosen_ver" ]]; then
                        [[ "$DEBUG_POM" == 1 ]] && echo "[DEDUP] skip $coord (already kept $dedup_key:$chosen_ver)" >&2
                        mark_processed "$coord"
                        continue
                    fi
                    ;;
                highest)
                    if version_gt "$VERSION" "$chosen_ver"; then
                        old_path="${TARGET_DIR}/${ARTIFACT_ID}-${chosen_ver}.jar"
                        if [[ -f "$old_path" ]]; then
                            echo "[DEDUP] replacing $dedup_key:$chosen_ver with higher $VERSION" >&2
                            rm -f "$old_path" || true
                        fi
                        set_dedup_version "$dedup_key" "$VERSION"
                    else
                        if [[ "$VERSION" != "$chosen_ver" ]]; then
                            [[ "$DEBUG_POM" == 1 ]] && echo "[DEDUP] skip $coord (higher or equal version $chosen_ver already selected)" >&2
                            mark_processed "$coord"
                            continue
                        fi
                    fi
                    ;;
                *) ;;
            esac
        else
            set_dedup_version "$dedup_key" "$VERSION"
        fi
    fi
    GROUP_PATH=${GROUP_ID//./\/}
    JAR_FILE="${ARTIFACT_ID}-${VERSION}.jar"
    DEST_FILE="${TARGET_DIR}/${JAR_FILE}"
    if is_excluded_artifact "$ARTIFACT_ID"; then
        echo "[SKIP] excluded by pattern: ${ARTIFACT_ID} (version ${VERSION})" >&2
        mark_processed "$coord"
        continue
    fi
    if [[ -f "$DEST_FILE" ]]; then
        echo "Already present: $JAR_FILE" >&2
    else
        URL="${MAVEN_REPO_URL}/${GROUP_PATH}/${ARTIFACT_ID}/${VERSION}/${JAR_FILE}"
        echo "Fetching ${JAR_FILE}..." >&2
        if download "$URL" "$DEST_FILE"; then
            echo "Downloaded ${JAR_FILE}" >&2
        else
            echo "Failed to download ${JAR_FILE} from ${URL}" >&2
            rm -f "$DEST_FILE" || true
        fi
    fi
    if [[ "$RESOLVE_TRANSITIVE" == 1 ]]; then
        echo "Resolving transitives for $coord" >&2
        while IFS= read -r dep; do
            [[ -z "$dep" ]] && continue
            if ! is_processed "$dep"; then
                # Avoid duplicates already queued
                skip=0
                for q in "${queue[@]-}"; do [[ "$q" == "$dep" ]] && { skip=1; break; }; done
                [[ $skip == 0 ]] && queue+=("$dep")
            fi
        done < <(fetch_pom_and_list_dependencies "$coord")
    fi
    mark_processed "$coord"
done

echo "Done. Total unique artifacts processed: $(wc -l <"$processed_file" 2>/dev/null || echo 0)" >&2
if [[ "$DEDUP_ARTIFACTS" == 1 && -s "$DEDUP_MAP_FILE" ]]; then
    echo "Deduplicated artifacts kept (group:artifact -> version):" >&2
    awk '{printf "  %s -> %s\n", $1, $2}' "$DEDUP_MAP_FILE" | sort -u >&2
    # Cleanup older versions still present in TARGET_DIR for chosen artifacts
    while read -r k v; do
        [[ -z "$k" || -z "$v" ]] && continue
        art="${k#*:}"
        # remove jars for same artifact with different versions
        for f in "${TARGET_DIR}/${art}-"*.jar; do
            [[ -e "$f" ]] || continue
            base="$(basename "$f")"
            if [[ "$base" != "${art}-${v}.jar" ]]; then
                echo "[DEDUP] removing extra version $base (kept ${art}-${v}.jar)" >&2
                rm -f -- "$f" || true
            fi
        done
    done < "$DEDUP_MAP_FILE"
fi
if [[ "$RESOLVE_TRANSITIVE" == 1 && "$DEBUG_POM" != 1 ]]; then
    rm -rf "$TMP_DIR"
else
    echo "(debug) preserved tmp dir: $TMP_DIR" >&2
fi
