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
    tools="--help migrate-bq verify oauth-token mysql app-engine alembic sync-consents edit-config fix-dup-pids genomic resource rdr-docs"
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
        rtool)
            COMPREPLY=( $(compgen -W "${tools}" -- ${cur}) )
            return 0
            ;;
        verify)
            COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            return 0
            ;;
        setup-local-db)
            COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            return 0
            ;;
        migrate-bq)
            # These are options specific to this tool.
            local toolopts="--dataset --delete"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        sync-consents)
            # These are options specific to this tool.
            local toolopts="--org-id --destination-bucket --dry-run --date-limit --end-date --zip-files --all-va --all-files --pid-file"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        edit-config)
            # These are options specific to this tool.
            local toolopts="--key --base-config --jira-ticket"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        fix-dup-pids)
            # These are options specific to this tool.
            local toolopts="--csv --participant --fix-biobank-orders --fix-physical-measurements --fix-signup-time"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        mysql)
            # These are options specific to this tool.
            local toolopts="export"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        export)
            # mysql list command
            if echo ${COMP_WORDS[@]} | grep -w "mysql" > /dev/null; then
              local toolopts="--help --database --bucket-uri --format"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        alembic)
            # These are options specific to this tool.
            local toolopts="--quiet"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        app-engine)
            # These are options specific to this tool.
            local toolopts="--git-project list deploy split-traffic config"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        deploy)
            # app-engine deploy command
            if echo ${COMP_WORDS[@]} | grep -w "app-engine" > /dev/null; then
              local toolopts="--help --git-branch --deploy-as --services --no-promote --quiet"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        list)
            # app-engine list command
            if echo ${COMP_WORDS[@]} | grep -w "app-engine" > /dev/null; then
              local toolopts="--help --running-only"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        split-traffic)
            # app-engine split-traffic command
            if echo ${COMP_WORDS[@]} | grep -w "app-engine" > /dev/null; then
              local toolopts="--help --quiet --service --versions --split-by"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        config)
            # app-engine split-traffic command
            if echo ${COMP_WORDS[@]} | grep -w "app-engine" > /dev/null; then
              local toolopts="--help --key --compare --update --to-file"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        genomic)
            # These are options specific to this tool.
            local toolopts="resend generate-manifest control-sample manual-sample"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        resend)
            # genomic resend command
            if echo ${COMP_WORDS[@]} | grep -w "genomic" > /dev/null; then
              local toolopts="--help --manifest --csv --sample"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        generate-manifest)
            # genomic generate-manifest command
            if echo ${COMP_WORDS[@]} | grep -w "genomic" > /dev/null; then
              local toolopts="--help --manifest --cohort"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
          control-sample)
            # genomic generate-manifest command
            if echo ${COMP_WORDS[@]} | grep -w "genomic" > /dev/null; then
              local toolopts="--help --csv --dryrun"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
          manual-sample)
            # genomic generate-manifest command
            if echo ${COMP_WORDS[@]} | grep -w "genomic" > /dev/null; then
              local toolopts="--help --csv --dryrun"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        test-jira)
            COMPREPLY=( $(compgen -W "${tools}" -- ${cur}) )
            return 0
            ;;
        resource)
            # These are options specific to this tool.
            local toolopts="rebuild-pids"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        rebuild-pids)
          # These are options specific to this tool.
          local toolopts="--pid --batch --all-pids --from-file"
          COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
          return 0
          ;;
        rdr-docs)
            # These are options specific to this tool.
            local toolopts="--help build list update"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        build)
            # rdr-docs build command
            if echo ${COMP_WORDS[@]} | grep -w "rdr-docs" > /dev/null; then
              local toolopts="--help --slug --no-wait"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        list)
            # rdr-docs list command
            if echo ${COMP_WORDS[@]} | grep -w "rdr-docs" > /dev/null; then
              local toolopts="--help --build --version --default-tag"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        update)
            # rdr-docs update command
            if echo ${COMP_WORDS[@]} | grep -w "rdr-docs" > /dev/null; then
              local toolopts="--help --latest"
              COMPREPLY=( $(compgen -W "${toolopts}" -- ${cur}) )
            else
              COMPREPLY=( $(compgen -W "${stdopts}" -- ${cur}) )
            fi
            return 0
            ;;
        resurrect)
            # These are options specific to this tool.
            local toolopts="--help --pid --reason --reason-desc"
            COMPREPLY=( $(compgen -W "${stdopts} ${toolopts}" -- ${cur}) )
            return 0
            ;;
        *)
        ;;
    esac

   COMPREPLY=($(compgen -W "${tools}" -- ${cur}))
   return 0
}
complete -F _python python
complete -F _python rtool

