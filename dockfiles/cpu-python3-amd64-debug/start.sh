#!/bin/bash
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

set -e

# The _log function is used for everything this script wants to log.
# It will always log errors and warnings but can be silenced for other messages
# by setting the JUPYTER_DOCKER_STACKS_QUIET environment variable.
_log () {
    if [[ "$*" == "ERROR:"* ]] || [[ "$*" == "WARNING:"* ]] || [[ "${JUPYTER_DOCKER_STACKS_QUIET}" == "" ]]; then
        echo "$@"
    fi
}
_log "Entered start.sh with args:" "$@"

# A helper function to unset env vars listed in the value of the env var
# JUPYTER_ENV_VARS_TO_UNSET.
unset_explicit_env_vars () {
    if [ -n "${JUPYTER_ENV_VARS_TO_UNSET}" ]; then
        for env_var_to_unset in $(echo "${JUPYTER_ENV_VARS_TO_UNSET}" | tr ',' ' '); do
            _log "Unset ${env_var_to_unset} due to JUPYTER_ENV_VARS_TO_UNSET"
            unset "${env_var_to_unset}"
        done
        unset JUPYTER_ENV_VARS_TO_UNSET
    fi
}

ulimit -c unlimited

# Default to starting bash if no command was specified
if [ $# -eq 0 ]; then
    cmd=( "bash" )
else
    cmd=( "$@" )
fi

# Backwards compatibility: `start.sh` is executed by default in ENTRYPOINT
# so it should no longer be specified in CMD
if [ "${_START_SH_EXECUTED}" = "1" ]; then
    _log "WARNING: start.sh is the default ENTRYPOINT, do not include it in CMD"
    _log "Executing the command:" "${cmd[@]}"
    exec "${cmd[@]}"
else
    export _START_SH_EXECUTED=1
fi


# NOTE: This hook will run as the user the container was started with!
# shellcheck disable=SC1091
source /usr/local/bin/run-hooks.sh /usr/local/bin/start-notebook.d

# If the container started as the root user, then we have permission to refit
# the jovyan user, and ensure file permissions, grant sudo rights, and such
# things before we run the command passed to start.sh as the desired user
# (NB_USER).
#
if [ "$(id -u)" == 0 ]; then
    # Environment variables:
    # - NB_USER: the desired username and associated home folder
    # - NB_UID: the desired user id
    # - NB_GID: a group id we want our user to belong to
    # - NB_GROUP: a group name we want for the group
    # - GRANT_SUDO: a boolean ("1" or "yes") to grant the user sudo rights
    # - CHOWN_HOME: a boolean ("1" or "yes") to chown the user's home folder
    # - CHOWN_EXTRA: a comma-separated list of paths to chown
    # - CHOWN_HOME_OPTS / CHOWN_EXTRA_OPTS: arguments to the chown commands

    # Refit the jovyan user to the desired user (NB_USER)
    if id jovyan &> /dev/null; then
        if ! usermod --home "/home/${NB_USER}" --login "${NB_USER}" jovyan 2>&1 | grep "no changes" > /dev/null; then
            _log "Updated the jovyan user:"
            _log "- username: jovyan       -> ${NB_USER}"
            _log "- home dir: /home/jovyan -> /home/${NB_USER}"
        fi
    elif ! id -u "${NB_USER}" &> /dev/null; then
        _log "ERROR: Neither the jovyan user nor '${NB_USER}' exists. This could be the result of stopping and starting, the container with a different NB_USER environment variable."
        exit 1
    fi
    # Ensure the desired user (NB_USER) gets its desired user id (NB_UID) and is
    # a member of the desired group (NB_GROUP, NB_GID)
    if [ "${NB_UID}" != "$(id -u "${NB_USER}")" ] || [ "${NB_GID}" != "$(id -g "${NB_USER}")" ]; then
        _log "Update ${NB_USER}'s UID:GID to ${NB_UID}:${NB_GID}"
        # Ensure the desired group's existence
        if [ "${NB_GID}" != "$(id -g "${NB_USER}")" ]; then
            groupadd --force --gid "${NB_GID}" --non-unique "${NB_GROUP:-${NB_USER}}"
        fi
        # Recreate the desired user as we want it
        userdel "${NB_USER}"
        useradd --no-log-init --home "/home/${NB_USER}" --shell /bin/bash --uid "${NB_UID}" --gid "${NB_GID}" --groups 100 "${NB_USER}"
    fi
    # Update the home directory if the desired user (NB_USER) is root and the
    # desired user id (NB_UID) is 0 and the desired group id (NB_GID) is 0.
    if [ "${NB_USER}" = "root" ] && [ "${NB_UID}" = "$(id -u "${NB_USER}")" ] && [ "${NB_GID}" = "$(id -g "${NB_USER}")" ]; then
        sed -i "s|/root|/home/root|g" /etc/passwd
        # Do not preserve ownership in rootless mode
        CP_OPTS="-a --no-preserve=ownership"
    fi

    # Move or symlink the jovyan home directory to the desired user's home
    # directory if it doesn't already exist, and update the current working
    # directory to the new location if needed.
    if [[ "${NB_USER}" != "jovyan" ]]; then
        if [[ ! -e "/home/${NB_USER}" ]]; then
            _log "Attempting to copy /home/jovyan to /home/${NB_USER}..."
            mkdir "/home/${NB_USER}"
            # shellcheck disable=SC2086
            if cp ${CP_OPTS:--a} /home/jovyan/. "/home/${NB_USER}/"; then
                _log "Success!"
            else
                _log "Failed to copy data from /home/jovyan to /home/${NB_USER}!"
                _log "Attempting to symlink /home/jovyan to /home/${NB_USER}..."
                if ln -s /home/jovyan "/home/${NB_USER}"; then
                    _log "Success creating symlink!"
                else
                    _log "ERROR: Failed copy data from /home/jovyan to /home/${NB_USER} or to create symlink!"
                    exit 1
                fi
            fi
        fi
        # Ensure the current working directory is updated to the new path
        if [[ "${PWD}/" == "/home/jovyan/"* ]]; then
            new_wd="/home/${NB_USER}/${PWD:13}"
            _log "Changing working directory to ${new_wd}"
            cd "${new_wd}"
        fi
    fi

    # Optionally ensure the desired user gets filesystem ownership of its home
    # folder and/or additional folders
    if [[ "${CHOWN_HOME}" == "1" || "${CHOWN_HOME}" == "yes" ]]; then
        _log "Ensuring /home/${NB_USER} is owned by ${NB_UID}:${NB_GID} ${CHOWN_HOME_OPTS:+(chown options: ${CHOWN_HOME_OPTS})}"
        # shellcheck disable=SC2086
        chown ${CHOWN_HOME_OPTS} "${NB_UID}:${NB_GID}" "/home/${NB_USER}"
    fi
    if [ -n "${CHOWN_EXTRA}" ]; then
        for extra_dir in $(echo "${CHOWN_EXTRA}" | tr ',' ' '); do
            _log "Ensuring ${extra_dir} is owned by ${NB_UID}:${NB_GID} ${CHOWN_EXTRA_OPTS:+(chown options: ${CHOWN_EXTRA_OPTS})}"
            # shellcheck disable=SC2086
            chown ${CHOWN_EXTRA_OPTS} "${NB_UID}:${NB_GID}" "${extra_dir}"
        done
    fi

    # Prepend ${CONDA_DIR}/bin to sudo secure_path
    sed -r "s#Defaults\s+secure_path\s*=\s*\"?([^\"]+)\"?#Defaults secure_path=\"${CONDA_DIR}/bin:\1\"#" /etc/sudoers | grep secure_path > /etc/sudoers.d/path

    # Optionally grant passwordless sudo rights for the desired user
    if [[ "${GRANT_SUDO}" == "1" || "${GRANT_SUDO}" == "yes" ]]; then
        _log "Granting ${NB_USER} passwordless sudo rights!"
        echo "${NB_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/added-by-start-script
    fi

    # NOTE: This hook is run as the root user!
    # shellcheck disable=SC1091
    source /usr/local/bin/run-hooks.sh /usr/local/bin/before-notebook.d
    unset_explicit_env_vars

    _log "Running as ${NB_USER}:" "${cmd[@]}"
    # if [[ "${cmd[0]}" == *.sh ]]; then
    if [[ -z "${RUN_IN_GDB}" ]]; then #
        _log "Will NOT be running script or executable in GDB."
        if [[ "${cmd[0]}" == *.sh ]]; then
            sudo --preserve-env --set-home --user "${NB_USER}" \
                          LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
                          PATH="${PATH}" \
                          PYTHONPATH="${PYTHONPATH:-}" \
                          bash -x "${cmd[@]}" 2>&1
        else
            "${cmd[@]}" 2>&1
        fi
    else
        if [[ "${cmd[0]}" == *.sh ]]; then
            _log "Running bash script in GDB: " "${cmd[@]}"
            sudo --preserve-env --set-home --user "${NB_USER}" \
                LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
                PATH="${PATH}" \
                PYTHONPATH="${PYTHONPATH:-}" \
                gdb -batch -ex "run" -ex "bt" -ex "generate-core-file" --args bash -x "${cmd[@]}" 2>&1 | grep -v ^"No stack."$ || true
        else
            _log "Running executable in GDB: " "${cmd[@]}"
            sudo --preserve-env --set-home --user "${NB_USER}" \
                LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
                PATH="${PATH}" \
                PYTHONPATH="${PYTHONPATH:-}" \
                RAND_STR="$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)"
                gdb -batch -ex "run" -ex "bt" -ex "generate-core-file core-$RAND_STR" --args "${cmd[@]}" 2>&1 | grep -v ^"No stack."$ || true
        fi
    fi
        # Notes on how we ensure that the environment that this container is started
        # with is preserved (except vars listed in JUPYTER_ENV_VARS_TO_UNSET) when
        # we transition from running as root to running as NB_USER.
        #
        # - We use `sudo` to execute the command as NB_USER. What then
        #   happens to the environment will be determined by configuration in
        #   /etc/sudoers and /etc/sudoers.d/* as well as flags we pass to the sudo
        #   command. The behavior can be inspected with `sudo -V` run as root.
        #
        #   ref: `man sudo`    https://linux.die.net/man/8/sudo
        #   ref: `man sudoers` https://www.sudo.ws/docs/man/sudoers.man/
        #
        # - We use the `--preserve-env` flag to pass through most environment
        #   variables, but understand that exceptions are caused by the sudoers
        #   configuration: `env_delete` and `env_check`.
        #
        # - We use the `--set-home` flag to set the HOME variable appropriately.
        #
        # - To reduce the default list of variables deleted by sudo, we could have
        #   used `env_delete` from /etc/sudoers. It has a higher priority than the
        #   `--preserve-env` flag and the `env_keep` configuration.
        #
        # - We preserve LD_LIBRARY_PATH, PATH and PYTHONPATH explicitly. Note however that sudo
        #   resolves `${cmd[@]}` using the "secure_path" variable we modified
        #   above in /etc/sudoers.d/path. Thus PATH is irrelevant to how the above
        #   sudo command resolves the path of `${cmd[@]}`. The PATH will be relevant
        #   for resolving paths of any subprocesses spawned by `${cmd[@]}`.

# The container didn't start as the root user, so we will have to act as the
# user we started as.
else
    # Warn about misconfiguration of: granting sudo rights
    if [[ "${GRANT_SUDO}" == "1" || "${GRANT_SUDO}" == "yes" ]]; then
        _log "WARNING: container must be started as root to grant sudo permissions!"
    fi

    JOVYAN_UID="$(id -u jovyan 2>/dev/null)"  # The default UID for the jovyan user
    JOVYAN_GID="$(id -g jovyan 2>/dev/null)"  # The default GID for the jovyan user

    # Attempt to ensure the user uid we currently run as has a named entry in
    # the /etc/passwd file, as it avoids software crashing on hard assumptions
    # on such entry. Writing to the /etc/passwd was allowed for the root group
    # from the Dockerfile during the build.
    #
    # ref: https://github.com/jupyter/docker-stacks/issues/552
    if ! whoami &> /dev/null; then
        _log "There is no entry in /etc/passwd for our UID=$(id -u). Attempting to fix..."
        if [[ -w /etc/passwd ]]; then
            _log "Renaming old jovyan user to nayvoj ($(id -u jovyan):$(id -g jovyan))"

            # We cannot use "sed --in-place" since sed tries to create a temp file in
            # /etc/ and we may not have write access. Apply sed on our own temp file:
            sed --expression="s/^jovyan:/nayvoj:/" /etc/passwd > /tmp/passwd
            echo "${NB_USER}:x:$(id -u):$(id -g):,,,:/home/jovyan:/bin/bash" >> /tmp/passwd
            cat /tmp/passwd > /etc/passwd
            rm /tmp/passwd

            _log "Added new ${NB_USER} user ($(id -u):$(id -g)). Fixed UID!"

            if [[ "${NB_USER}" != "jovyan" ]]; then
                _log "WARNING: user is ${NB_USER} but home is /home/jovyan. You must run as root to rename the home directory!"
            fi
        else
            _log "WARNING: unable to fix missing /etc/passwd entry because we don't have write permission. Try setting gid=0 with \"--user=$(id -u):0\"."
        fi
    fi

    # Warn about misconfiguration of: desired username, user id, or group id.
    # A misconfiguration occurs when the user modifies the default values of
    # NB_USER, NB_UID, or NB_GID, but we cannot update those values because we
    # are not root.
    if [[ "${NB_USER}" != "jovyan" && "${NB_USER}" != "$(id -un)" ]]; then
        _log "WARNING: container must be started as root to change the desired user's name with NB_USER=\"${NB_USER}\"!"
    fi
    if [[ "${NB_UID}" != "${JOVYAN_UID}" && "${NB_UID}" != "$(id -u)" ]]; then
        _log "WARNING: container must be started as root to change the desired user's id with NB_UID=\"${NB_UID}\"!"
    fi
    if [[ "${NB_GID}" != "${JOVYAN_GID}" && "${NB_GID}" != "$(id -g)" ]]; then
        _log "WARNING: container must be started as root to change the desired user's group id with NB_GID=\"${NB_GID}\"!"
    fi

    # Warn if the user isn't able to write files to ${HOME}
    if [[ ! -w /home/jovyan ]]; then
        _log "WARNING: no write access to /home/jovyan. Try starting the container with group 'users' (100), e.g. using \"--group-add=users\"."
    fi

    # NOTE: This hook is run as the user we started the container as!
    # shellcheck disable=SC1091
    source /usr/local/bin/run-hooks.sh /usr/local/bin/before-notebook.d
    unset_explicit_env_vars

    _log "Executing the specified command:" "${cmd[@]}"
    # if [[ "${cmd[0]}" == *.sh ]]; then
    if [[ -z "${RUN_IN_GDB}" ]]; then # If RUN_IN_GDB is not set, then don't run in GDB.
        _log "Will NOT be running script or executable in GDB."
        "${cmd[@]}" 2>&1
    else
        _log "Running executable in GDB: " "${cmd[@]}"
        RAND_STR=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13; echo)
        HST=$(hostname -i)
        TS=$(date +"%Y-%m-%d_%H-%M-%S")
        screen -S gdb-kernel gdb -ex "run" -ex "generate-core-file /cores/core-$HST-$TS" -ex "bt" -ex "py-bt" --args "${cmd[@]}" 2>&1
    # else
    #    "${cmd[@]}" 2>&1
    fi

    _log "Notebook exited with code $?"
    sleep 2
    _log "Exiting"
fi