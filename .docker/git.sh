#!/bin/bash
# git.sh: Configura git globalmente dentro del contenedor
if [ -n "$GIT_USER_NAME" ] && [ -n "$GIT_USER_EMAIL" ]; then
    git config --global user.name "$GIT_USER_NAME"
    git config --global user.email "$GIT_USER_EMAIL"
    echo "Git configurado: $GIT_USER_NAME <$GIT_USER_EMAIL>"
else
    echo "Faltan variables GIT_USER_NAME o GIT_USER_EMAIL. Configura git manualmente."
fi

