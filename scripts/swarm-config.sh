#!/bin/sh
# Trusted configuration helpers, sourced by swarm.sh. Configuration is never sourced.
swarm_config_keys='KPL_STACK_NAME KPL_CONTROL_NODE_ID KPL_PEER_NETWORK KPL_PEER_SUBNET KPL_IMAGE KPL_AGENT_CAPACITY KPL_AGENT_METRICS_PORT KPL_USER KPL_PASSWORD GRAFANA_ADMIN_USER GRAFANA_ADMIN_PASSWORD KPL_HTTP_PORT PROMETHEUS_PORT GRAFANA_PORT KPL_MIN_AGENTS KPL_DOCKER_TIMEOUT KPL_IMAGE_BUILD_TIMEOUT KPL_IMAGE_PUSH_TIMEOUT KPL_IMAGE_PULL_TIMEOUT KPL_CONTROLLER_STOP_TIMEOUT KPL_AGENT_STOP_TIMEOUT'

swarm_config_key() {
    case "$1" in ''|*[!A-Z0-9_]*) return 1 ;; esac
    case " $swarm_config_keys " in *" $1 "*) return 0 ;; *) return 1 ;; esac
}

swarm_setting_value() {
    # Preserve trailing newlines from environment values so validation can reject
    # them, rather than silently changing a token during command substitution.
    sc_value=$(printenv "$1" && printf '.') || return 1
    sc_value=${sc_value%.}
    sc_value=${sc_value%?}
}

swarm_load_config() {
    [ "$2" != yes ] || [ -f "$1" ] || fail "Config not found: $1"
    [ -f "$1" ] || return 0
    sc_line_number=0
    sc_legacy_token=no
    sc_cr=$(printf '\r')
    while IFS= read -r sc_line || [ -n "$sc_line" ]; do
        sc_line_number=$((sc_line_number + 1))
        sc_line=${sc_line%"$sc_cr"}
        case "$sc_line" in ''|'#'*) continue ;; esac
        case "$sc_line" in *=*) sc_key=${sc_line%%=*}; sc_value=${sc_line#*=} ;;
            *) fail "Invalid config entry at line $sc_line_number." ;; esac
        # Old files can be rewritten by configure; the retired token is never used.
        if [ "$sc_key" = KPL_API_TOKEN ]; then sc_legacy_token=yes; continue; fi
        swarm_config_key "$sc_key" || fail "Unsupported config key at line $sc_line_number."
        case "$sc_value" in \"*\") sc_value=${sc_value#\"}; sc_value=${sc_value%\"} ;;
            \'*\') sc_value=${sc_value#\'}; sc_value=${sc_value%\'} ;; esac
        if ! printenv "$sc_key" >/dev/null 2>&1; then export "$sc_key=$sc_value"; fi
    done < "$1"
}

swarm_validate_setting() {
    swarm_config_key "$1" || fail "Unsupported config key: $1"
    # The file format is one literal setting per line. Never print secret values.
    case "$2" in *'
'*|*"$(printf '\r')"*) fail "$1 must fit on one line." ;; esac
    case "$1" in
        KPL_STACK_NAME)
            case "$2" in ''|[!a-z0-9]*|*[!a-z0-9_-]*) fail 'Invalid KPL_STACK_NAME.' ;; esac ;;
        KPL_CONTROL_NODE_ID)
            case "$2" in ''|*[!a-zA-Z0-9]*) fail 'KPL_CONTROL_NODE_ID must be an exact node ID.' ;; esac ;;
        KPL_PEER_NETWORK)
            case "$2" in ''|-*|*[!a-zA-Z0-9_.-]*) fail 'Invalid KPL_PEER_NETWORK.' ;; esac ;;
        KPL_PEER_SUBNET)
            [ -n "$2" ] || return 0
            printf '%s\n' "$2" | awk '
                {
                    if (split($0, cidr, "/") != 2 || cidr[2] !~ /^[0-9]+$/ || cidr[2] ~ /^0[0-9]/ || cidr[2] < 1 || cidr[2] > 30) exit 1
                    if (split(cidr[1], octet, ".") != 4) exit 1
                    address=0
                    for (i=1; i<=4; i++) {
                        if (octet[i] !~ /^[0-9]+$/ || octet[i] ~ /^0[0-9]/ || octet[i] > 255) exit 1
                        address=address*256+octet[i]
                    }
                    block=2^(32-cidr[2])
                    if (address != int(address/block)*block) exit 1
                }
            ' || fail 'KPL_PEER_SUBNET must be a canonical IPv4 network CIDR with prefix 1 through 30.' ;;
        KPL_HTTP_PORT|KPL_AGENT_METRICS_PORT|PROMETHEUS_PORT|GRAFANA_PORT)
            case "$2" in ''|0*|*[!0-9]*) fail "$1 must be a port between 1 and 65535." ;; esac
            [ "$2" -le 65535 ] 2>/dev/null || fail "$1 must be a port between 1 and 65535." ;;
        KPL_AGENT_CAPACITY|KPL_MIN_AGENTS|KPL_DOCKER_TIMEOUT|KPL_IMAGE_BUILD_TIMEOUT|KPL_IMAGE_PUSH_TIMEOUT|KPL_IMAGE_PULL_TIMEOUT|KPL_CONTROLLER_STOP_TIMEOUT|KPL_AGENT_STOP_TIMEOUT)
            case "$2" in ''|0*|*[!0-9]*) fail "$1 must be a positive integer without leading zeros." ;; esac
            [ "$2" -gt 0 ] 2>/dev/null || fail "$1 exceeds the supported integer range." ;;
        KPL_IMAGE)
            sc_image=$2
            sc_digest=''
            case "$sc_image" in
                *@sha256:*)
                    sc_digest=${sc_image##*@sha256:}; sc_image=${sc_image%@sha256:*}
                    case "$sc_digest" in ''|*[!a-f0-9]*) fail 'KPL_IMAGE has an invalid sha256 digest.' ;; esac
                    [ "${#sc_digest}" -eq 64 ] || fail 'KPL_IMAGE sha256 digest must contain 64 hex digits.' ;;
                *@*) fail 'KPL_IMAGE digest must use sha256.' ;;
            esac
            case "${sc_image##*/}" in
                *:*)
                    sc_tag=${sc_image##*:}; sc_image=${sc_image%:*}
                    case "$sc_tag" in ''|[!a-zA-Z0-9_]*|*[!a-zA-Z0-9_.-]*) fail 'KPL_IMAGE has an invalid tag.' ;; esac
                    [ "${#sc_tag}" -le 128 ] || fail 'KPL_IMAGE tag cannot exceed 128 characters.' ;;
                *) [ -n "$sc_digest" ] || fail 'KPL_IMAGE requires an explicit tag or sha256 digest.' ;;
            esac
            case "$sc_image" in ''|-*|/*|*/|*//*|*://*|*[!a-zA-Z0-9._:/-]*) fail 'KPL_IMAGE has an invalid repository reference.' ;; esac ;;
        KPL_USER)
            case "$2" in ''|[[:space:]]*|*[[:space:]]) fail 'KPL_USER must not be blank or have surrounding whitespace.' ;; esac
            [ "${#2}" -le 256 ] || fail 'KPL_USER cannot exceed 256 characters.' ;;
        KPL_PASSWORD)
            [ -n "$2" ] || fail 'KPL_PASSWORD cannot be empty.'
            [ "${#2}" -le 4096 ] || fail 'KPL_PASSWORD cannot exceed 4096 characters.' ;;
        GRAFANA_ADMIN_USER|GRAFANA_ADMIN_PASSWORD)
            [ -n "$2" ] || fail "$1 cannot be empty." ;;
    esac
}

swarm_validate_port_conflicts() {
    sc_http_port=${KPL_HTTP_PORT:-8080}
    sc_agent_metrics_port=${KPL_AGENT_METRICS_PORT:-9091}
    sc_prometheus_port=${PROMETHEUS_PORT:-9090}
    sc_grafana_port=${GRAFANA_PORT:-3000}
    swarm_validate_setting KPL_HTTP_PORT "$sc_http_port"
    swarm_validate_setting KPL_AGENT_METRICS_PORT "$sc_agent_metrics_port"
    swarm_validate_setting PROMETHEUS_PORT "$sc_prometheus_port"
    swarm_validate_setting GRAFANA_PORT "$sc_grafana_port"
    case "$sc_agent_metrics_port" in
        "$sc_http_port") fail 'KPL_AGENT_METRICS_PORT conflicts with KPL_HTTP_PORT.' ;;
        "$sc_prometheus_port") fail 'KPL_AGENT_METRICS_PORT conflicts with PROMETHEUS_PORT.' ;;
        "$sc_grafana_port") fail 'KPL_AGENT_METRICS_PORT conflicts with GRAFANA_PORT.' ;;
        2377|7946) fail 'KPL_AGENT_METRICS_PORT conflicts with a Docker Swarm TCP port (2377 or 7946).' ;;
    esac
}

swarm_config_defaults() {
    export KPL_STACK_NAME=${KPL_STACK_NAME:-kpl}
    export KPL_PEER_NETWORK=${KPL_PEER_NETWORK:-$KPL_STACK_NAME-peers}
    export KPL_AGENT_CAPACITY=${KPL_AGENT_CAPACITY:-20} KPL_MIN_AGENTS=${KPL_MIN_AGENTS:-1}
    export KPL_USER=${KPL_USER:-admin}
    export GRAFANA_ADMIN_USER=${GRAFANA_ADMIN_USER:-admin}
    export KPL_HTTP_PORT=${KPL_HTTP_PORT:-8080} KPL_AGENT_METRICS_PORT=${KPL_AGENT_METRICS_PORT:-9091} PROMETHEUS_PORT=${PROMETHEUS_PORT:-9090} GRAFANA_PORT=${GRAFANA_PORT:-3000}
    export KPL_DOCKER_TIMEOUT=${KPL_DOCKER_TIMEOUT:-60}
    export KPL_IMAGE_BUILD_TIMEOUT=${KPL_IMAGE_BUILD_TIMEOUT:-1800} KPL_IMAGE_PUSH_TIMEOUT=${KPL_IMAGE_PUSH_TIMEOUT:-600} KPL_IMAGE_PULL_TIMEOUT=${KPL_IMAGE_PULL_TIMEOUT:-300}
    export KPL_CONTROLLER_STOP_TIMEOUT=${KPL_CONTROLLER_STOP_TIMEOUT:-660} KPL_AGENT_STOP_TIMEOUT=${KPL_AGENT_STOP_TIMEOUT:-240}
}

swarm_config_stage() {
    umask 077
    # Keep in-repository staging under the existing .env.* Docker exclusion too.
    sc_stage=$(mktemp -d "$(dirname -- "$env_file")/.env.swarm.stage.XXXXXX") || fail 'Cannot create the private config staging directory.'
    trap 'rm -f -- "$sc_stage/config" "$sc_stage/original"; rmdir -- "$sc_stage"; if [ -n "$sc_lock" ]; then rmdir -- "$sc_lock"; fi' 0
    trap 'exit 1' HUP INT TERM
}

swarm_config_command() (
    sc_command=$1; shift
    sc_lock=''
    case "$sc_command" in
        config|credentials) [ "$#" -eq 0 ] || fail "$sc_command takes no arguments." ;;
        configure) [ "$#" -gt 0 ] || fail 'configure requires at least one KEY=VALUE setting.' ;;
    esac
    # Validate the complete input before querying Docker or changing any file.
    sc_seen=' '
    for sc_argument do
        case "$sc_argument" in *=*) sc_key=${sc_argument%%=*}; sc_value=${sc_argument#*=} ;;
            *) fail 'Use literal KEY=VALUE arguments with init or configure.' ;; esac
        swarm_validate_setting "$sc_key" "$sc_value"
        case "$sc_seen" in *" $sc_key "*) fail "Duplicate setting: $sc_key" ;; esac
        sc_seen="$sc_seen$sc_key "
    done
    if [ "$sc_command" = init ]; then
        [ ! -e "$env_file" ] && [ ! -L "$env_file" ] || fail "Config already exists: $env_file"
    elif [ "$sc_command" = configure ]; then
        [ -f "$env_file" ] && [ ! -L "$env_file" ] || fail 'configure requires an existing regular config file, not a symlink.'
        swarm_config_stage
        # Serialize helper writers from the initial read through the rename.
        mkdir -- "$env_file.lock" || fail "Another configuration edit is active, or a stale lock exists: $env_file.lock"
        sc_lock=$env_file.lock
        cp -- "$env_file" "$sc_stage/original"
        # A saved edit changes only the requested settings, not unrelated values
        # inherited from the caller. Runtime commands still give environment priority.
        for sc_key in $swarm_config_keys; do unset "$sc_key"; done
        swarm_load_config "$sc_stage/original" yes
    else
        swarm_load_config "$env_file" yes
    fi
    for sc_argument do export "$sc_argument"; done
    swarm_config_defaults
    for sc_key in $swarm_config_keys; do
        if swarm_setting_value "$sc_key"; then swarm_validate_setting "$sc_key" "$sc_value"; fi
    done
    swarm_validate_port_conflicts
    if [ "$sc_command" = init ]; then
        command -v docker >/dev/null 2>&1 || fail 'Docker CLI is required.'
        command -v timeout >/dev/null 2>&1 || fail 'Linux timeout (coreutils) is required.'
        [ "$(timeout -s TERM -k 5 "$KPL_DOCKER_TIMEOUT" docker info --format '{{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}')" = 'active true' ] || fail 'Run against an active Swarm manager.'
        if [ -z "${KPL_CONTROL_NODE_ID:-}" ]; then
            KPL_CONTROL_NODE_ID=$(timeout -s TERM -k 5 "$KPL_DOCKER_TIMEOUT" docker info --format '{{.Swarm.NodeID}}') || fail 'Cannot identify the current Swarm manager.'
            export KPL_CONTROL_NODE_ID
        fi
        if [ -z "${GRAFANA_ADMIN_PASSWORD:-}" ]; then
            GRAFANA_ADMIN_PASSWORD=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
            [ "${#GRAFANA_ADMIN_PASSWORD}" -eq 64 ] || fail 'Could not generate the Grafana password.'
            export GRAFANA_ADMIN_PASSWORD
        fi
        export KPL_IMAGE=${KPL_IMAGE:-registry.example.com/kpl-v3:v3}
    fi
    for sc_key in $swarm_config_keys; do
        if swarm_setting_value "$sc_key"; then swarm_validate_setting "$sc_key" "$sc_value"; fi
    done
    if { [ "$sc_command" = init ] || [ "$sc_command" = configure ]; } && [ -z "${KPL_PASSWORD:-}" ]; then
        KPL_PASSWORD=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
        [ "${#KPL_PASSWORD}" -eq 64 ] || fail 'Could not generate the dashboard password.'
        export KPL_PASSWORD
    fi
    case "$sc_command" in
        config)
            printf 'Effective configuration (environment overrides %s):\n' "$env_file"
            for sc_key in $swarm_config_keys; do
                swarm_setting_value "$sc_key" || continue
                case "$sc_key" in KPL_PASSWORD|GRAFANA_ADMIN_PASSWORD) sc_value='[redacted]' ;; esac
                printf '%s=%s\n' "$sc_key" "$sc_value"
            done
            exit 0 ;;
        credentials)
            : "${KPL_PASSWORD:?Dashboard password is not configured}" "${GRAFANA_ADMIN_PASSWORD:?Grafana password is not configured}"
            printf 'Credentials from the effective configuration; keep this output private.\n'
            printf 'KPL_USER=%s\nKPL_PASSWORD=%s\nGRAFANA_ADMIN_USER=%s\nGRAFANA_ADMIN_PASSWORD=%s\n' "$KPL_USER" "$KPL_PASSWORD" "$GRAFANA_ADMIN_USER" "$GRAFANA_ADMIN_PASSWORD"
            printf 'Grafana retains its existing username and synchronizes the original administrator password on startup.\n'
            exit 0 ;;
    esac
    # Stage beside the destination so rename is atomic and stays on one filesystem.
    # A private directory also keeps credentials inaccessible while being written.
    [ "$sc_command" = configure ] || swarm_config_stage
    {
        printf '# Literal KEY=VALUE entries; environment variables take precedence.\n'
        for sc_key in $swarm_config_keys; do
            swarm_setting_value "$sc_key" || continue
            # Preserve literal enclosing quotes through the loader's one-pair rule.
            case "$sc_value" in \"*\"|\'*\') printf "%s='%s'\n" "$sc_key" "$sc_value" ;;
                *) printf '%s=%s\n' "$sc_key" "$sc_value" ;; esac
        done
    } > "$sc_stage/config"
    chmod 600 "$sc_stage/config"
    if [ "$sc_command" = init ]; then
        # Link is an atomic create-if-absent; it cannot overwrite another init.
        ln -- "$sc_stage/config" "$env_file" || fail 'Could not create config (existing files are never overwritten).'
        printf 'Created %s (mode 0600). Review it with: sh scripts/swarm.sh config\n' "$env_file"
    else
        cmp -s "$env_file" "$sc_stage/original" || fail 'Config changed while editing; retry configure.'
        [ ! -L "$env_file" ] || fail 'Config became a symlink; refusing to replace it.'
        mv -f -- "$sc_stage/config" "$env_file"
        printf 'Updated %s (mode 0600); existing credentials are preserved unless explicitly changed.\n' "$env_file"
        if [ "$sc_legacy_token" = yes ]; then
            printf 'Migrated KPL_API_TOKEN to dashboard login settings; use credentials to view KPL_USER and KPL_PASSWORD.\n'
        fi
        printf 'Exported environment variables still override saved settings. Run deploy to apply changes to services.\n'
    fi
)
