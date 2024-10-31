set -e #
exec pipenv run python3 -m app.main "$@"
