# esther-spark-streaming

## Flux de release automatique (uv + semantic-release)

- Installer [uv](https://docs.astral.sh/uv/) puis installer les dépendances de dev : `uv sync --dev`.
- Utiliser les Conventional Commits (`feat:`, `fix:`, `refactor:`, `BREAKING CHANGE`, etc.) pour que semantic-release puisse calculer le next version.
- Lancer le bump de version + commit + tag localement : `uv run semantic-release version`.
- Publier le résultat : `git push --follow-tags`.
- Pour vérifier sans écrire de tag/commit, ajouter l’option `--noop` : `uv run semantic-release version --noop`.

Exemple de job CI minimal (GitHub Actions) :

```yaml
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync --dev
      - run: uv run semantic-release version
      - run: git push --follow-tags
```
