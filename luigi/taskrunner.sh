#!/bin/bash


source /luigi/.pyenv/bin/activate

cd /luigi/tasks

if [[ -f "requirements.txt" ]]
then
  pip install -r requirements.txt
fi

exec python -m luigi "$@"