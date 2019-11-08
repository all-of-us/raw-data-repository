#
# bash completion for rdr tools
# See: https://debian-administration.org/article/317/An_introduction_to_bash_completion_part_2
#
# Usage: run `. rdr_service/tools/tool_libs/tools.bash` or add to bash profile.
#
_python()
{
    local cur prev tools stdopts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"


    # These are the specific tools we support
    tools="--help migrate-bq verify oauth-token mysql app-engine"
    # These are the standard options all tools support.
    stdopts="--help --debug --log-file --project --account --service-account"

    #
    #  Complete the arguments to some of the basic commands.
    #
    case "${prev}" in
        python)
           COMPREPLY=( $(compgen -W "-m" -- ${cur}) )
           return 0
           ;;
        -m)
           COMPREPLY=( $(compgen -W "tools" -- ${cur}) )
           return 0
           ;;
        tools)
            COMPREPLY=( $(compgen -W "${tools}" -- ${cur}) )
            return 0
            ;;
        verify)
            COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            return 0
            ;;
        migrate-bq)
            # These are options specific to this tool.
            local toolopts="--dataset --delete"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        mysql)
            # These are options specific to this tool.
            local toolopts="--create-cloud-instance --change-passwords"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        app-engine)
            # These are options specific to this tool.
            local toolopts="list deploy split-traffic"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        deploy)
            # app-engine deploy command
            if echo ${COMP_WORDS[@]} | grep -w "app-engine" > /dev/null; then
              local toolopts="--git-branch --deploy-as --git-project --services --promote --quiet"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        list)
            # app-engine list command
            if echo ${COMP_WORDS[@]} | grep -w "app-engine" > /dev/null; then
              local toolopts="--running-only"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        split-traffic)
            # app-engine split-traffic command
            if echo ${COMP_WORDS[@]} | grep -w "app-engine" > /dev/null; then
              local toolopts="--quiet --service --versions --split-by"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        *)
        ;;
    esac

   COMPREPLY=($(compgen -W "${tools}" -- ${cur}))
   return 0
}
complete -F _python python

