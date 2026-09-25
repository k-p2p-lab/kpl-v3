# Bash completion for the `swarm` function installed by activate-swarm.sh.
# No bash-completion package, config evaluation or credential lookup is needed.

_kpl_swarm_offer() {
    local candidate
    for candidate in "$@"; do
        [[ $candidate == "$cur"* ]] && COMPREPLY+=("$candidate")
    done
    return 0
}

_kpl_swarm_files() {
    local candidate
    compopt -o filenames 2>/dev/null || true
    while IFS= read -r candidate; do
        COMPREPLY+=("$candidate")
    done < <(compgen -f -- "$cur")
}

# Read-only, bounded queries using the caller's Docker context. Cache briefly
# so repeated Tab presses do not continually contact an unavailable manager.
_kpl_swarm_docker() {
    local kind=$1 key now=${SECONDS:-0} output=''
    key="$kind|${DOCKER_CONTEXT-}|${DOCKER_HOST-}|${DOCKER_CONFIG-}|${DOCKER_TLS_VERIFY-}|${DOCKER_CERT_PATH-}"
    if [[ ${_KPL_SWARM_CACHE_KEY-} == "$key" ]] && (( now >= ${_KPL_SWARM_CACHE_AT:-0} && now - ${_KPL_SWARM_CACHE_AT:-0} < 5 )); then
        _KPL_SWARM_CANDIDATES=${_KPL_SWARM_CACHE_DATA-}
        return 0
    fi
    if command -v docker >/dev/null 2>&1 && command -v timeout >/dev/null 2>&1; then
        case $kind in
            nodes) output=$(command timeout -s TERM -k 1 1 docker node ls --format '{{.ID}} {{.Hostname}}' </dev/null 2>/dev/null) || output='' ;;
            contexts) output=$(command timeout -s TERM -k 1 1 docker context ls --format '{{.Name}}' </dev/null 2>/dev/null) || output='' ;;
        esac
    fi
    _KPL_SWARM_CACHE_KEY=$key
    _KPL_SWARM_CACHE_AT=$now
    _KPL_SWARM_CACHE_DATA=$output
    _KPL_SWARM_CANDIDATES=$output
}

_kpl_swarm_nodes() {
    local ids_only=${1:-no} id hostname used word used_index
    _kpl_swarm_docker nodes
    while read -r id hostname; do
        [[ -n $id ]] || continue
        used=no
        if [[ $ids_only == no ]]; then
            for (( used_index=node_start; used_index<COMP_CWORD; used_index++ )); do
                word=${COMP_WORDS[used_index]}
                [[ $word == "$id" || $word == "$hostname" ]] && used=yes
            done
        fi
        [[ $used == no ]] || continue
        _kpl_swarm_offer "$id"
        if [[ $ids_only == no && -n $hostname ]]; then _kpl_swarm_offer "$hostname"; fi
    done <<< "$_KPL_SWARM_CANDIDATES"
}

_kpl_swarm_complete() {
    COMPREPLY=()
    local cur=${COMP_WORDS[COMP_CWORD]-} prev='' index=1 node_start=1 command_name word component=controller
    local selected=no has_nodes=no tail_used=no context_used=no candidate key='' value_prefix=''
    (( COMP_CWORD >= 1 )) || return 0
    prev=${COMP_WORDS[COMP_CWORD-1]-}
    if [[ ${COMP_WORDS[index]-} == --env-file ]] && (( COMP_CWORD > index )); then
        if (( COMP_CWORD == index + 1 )); then _kpl_swarm_files; return 0; fi
        index=$((index + 2))
    fi
    if (( COMP_CWORD == index )); then
        _kpl_swarm_offer init configure config credentials nodes login publish check access logs scenario deploy status add-node remove-node remove help --help
        if (( index == 1 )); then _kpl_swarm_offer --env-file; fi
        return 0
    fi
    command_name=${COMP_WORDS[index]-}
    index=$((index + 1))
    node_start=$index
    case $command_name in
        deploy|add-node|remove-node)
            for (( ; index < COMP_CWORD; index++ )); do
                word=${COMP_WORDS[index]}
                case $word in --all|--all-excluding-self|--workers) selected=yes ;; *) has_nodes=yes ;; esac
            done
            [[ $selected == no ]] || return 0
            if [[ $has_nodes == no ]]; then _kpl_swarm_offer --all --all-excluding-self --workers; fi
            [[ $cur == --* ]] || _kpl_swarm_nodes
            ;;
        logs)
            if (( COMP_CWORD == index )); then
                _kpl_swarm_offer controller agent prometheus grafana access auth --tail
                return 0
            fi
            case ${COMP_WORDS[index]-} in
                --*) ;;
                *) component=${COMP_WORDS[index]}; index=$((index + 1)) ;;
            esac
            case $component in controller|agent|prometheus|grafana|access|auth) ;; *) return 0 ;; esac
            while (( index < COMP_CWORD )); do
                case ${COMP_WORDS[index]} in
                    --tail)
                        tail_used=yes
                        if (( index + 1 == COMP_CWORD )); then _kpl_swarm_offer 100 200 500 1000 all; return 0; fi ;;
                    --context)
                        [[ $component == access || $component == auth ]] || return 0
                        context_used=yes
                        if (( index + 1 == COMP_CWORD )); then
                            _kpl_swarm_docker contexts
                            while IFS= read -r candidate; do [[ -z $candidate ]] || _kpl_swarm_offer "$candidate"; done <<< "$_KPL_SWARM_CANDIDATES"
                            return 0
                        fi ;;
                    *) return 0 ;;
                esac
                index=$((index + 2))
            done
            if [[ $tail_used == no ]]; then _kpl_swarm_offer --tail; fi
            if [[ $context_used == no && ( $component == access || $component == auth ) ]]; then _kpl_swarm_offer --context; fi
            ;;
        publish)
            if (( COMP_CWORD == index )); then _kpl_swarm_offer --platforms
            elif (( COMP_CWORD == index + 1 )) && [[ $prev == --platforms ]]; then
                _kpl_swarm_offer linux/amd64 linux/arm64 linux/arm/v7 linux/amd64,linux/arm64
            fi ;;
        scenario)
            if (( COMP_CWORD == index )); then _kpl_swarm_files; fi ;;
        init|configure)
            # Bash normally splits KEY=value at '=' in COMP_WORDS. Support
            # both its default word breaks and shells that keep '=' intact.
            if [[ $cur == *=* && $cur != = ]]; then
                key=${cur%%=*}; value_prefix=$key=; cur=${cur#*=}
            elif [[ $cur == = ]]; then key=$prev; cur=''
            elif [[ $prev == = ]]; then key=${COMP_WORDS[COMP_CWORD-2]-}
            elif [[ $prev == *= ]]; then key=${prev%=}
            fi
            if [[ -n $key ]]; then
                if [[ $key == KPL_CONTROL_NODE_ID ]]; then
                    _kpl_swarm_nodes yes
                    if [[ -n $value_prefix ]]; then
                        for (( index=0; index<${#COMPREPLY[@]}; index++ )); do COMPREPLY[index]=$value_prefix${COMPREPLY[index]}; done
                    fi
                fi
                return 0
            fi
            compopt -o nospace 2>/dev/null || true
            _kpl_swarm_offer KPL_STACK_NAME= KPL_CONTROL_NODE_ID= KPL_PEER_NETWORK= KPL_PEER_SUBNET= KPL_IMAGE= KPL_AGENT_CAPACITY= KPL_RUN_MIN_FREE_BYTES= KPL_AGENT_METRICS_PORT= KPL_USER= KPL_PASSWORD= GRAFANA_ADMIN_USER= GRAFANA_ADMIN_PASSWORD= KPL_HTTP_PORT= PROMETHEUS_PORT= GRAFANA_PORT= KPL_MIN_AGENTS= KPL_DOCKER_TIMEOUT= KPL_IMAGE_BUILD_TIMEOUT= KPL_IMAGE_PUSH_TIMEOUT= KPL_IMAGE_PULL_TIMEOUT= KPL_CONTROLLER_STOP_TIMEOUT= KPL_AGENT_STOP_TIMEOUT=
            ;;
    esac
    return 0
}
