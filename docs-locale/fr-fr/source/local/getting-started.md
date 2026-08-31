---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Choisissez une méthode d'accès et créez votre premier graphe GitLab Orbit local."
title: Premiers pas avec GitLab Orbit Local
---

{{< details >}}

- Édition : Gratuite, GitLab Premium, GitLab Ultimate
- Offre : GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Statut : version bêta

{{< /details >}}

{{< history >}}

- [Introduit](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) dans GitLab 19.0 en tant que [version expérimentale](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Passage](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) en [version bêta](https://docs.gitlab.com/policy/development_stages_support/#beta) dans GitLab 19.1.

{{< /history >}}

GitLab Orbit Local s'exécute sur votre machine. Installez le binaire `orbit`, choisissez la méthode d'accès qui correspond à votre façon de travailler, puis exécutez votre première requête.

## Installation {#install}

Installez le binaire `orbit` directement avec le programme d'installation en une ligne, depuis npm, ou via le CLI GitLab (`glab`) si vous l'utilisez déjà.

Sur Linux, le programme d'installation utilise l'archive glibc par défaut et sélectionne automatiquement l'archive musl entièrement statique sur les distributions basées sur musl comme Alpine. Pour forcer l'archive Linux statique, passez `--libc musl`.

{{< tabs >}}

{{< tab title="macOS et Linux" >}}

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

Pour installer explicitement le binaire musl statique (par exemple sur un système glibc) :

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --libc musl
```

Ouvrez un nouveau terminal, puis vérifiez :

```shell
orbit help
```

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 | iex
```

Ouvrez un nouveau terminal, puis vérifiez :

```shell
orbit help
```

Si votre stratégie de point de terminaison restreint l'exécution de scripts distants, vous pouvez procéder à l'installation sans exécuter de script. L'archive de release Windows contient un unique fichier `orbit.exe` autonome, signé par GitLab Inc., de sorte que les stratégies d'autorisation d'applications peuvent l'autoriser par éditeur :

1. Depuis la [dernière release](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases), téléchargez `orbit-local-windows-x86_64.zip` et son fichier `.sha256`.
1. Vérifiez la somme de contrôle :

   ```powershell
   (Get-FileHash .\orbit-local-windows-x86_64.zip -Algorithm SHA256).Hash
   ```

   La sortie doit correspondre au hachage dans le fichier `.sha256`. La comparaison n'est pas sensible à la casse : `Get-FileHash` renvoie des majuscules et le fichier `.sha256` stocke des minuscules.
1. Effacez la marque du web (Mark of the Web). Un navigateur l'attache au téléchargement, l'extraction la transfère à `orbit.exe`, et la première exécution peut alors déclencher une invite SmartScreen ou être bloquée par la stratégie :

   ```powershell
   Unblock-File .\orbit-local-windows-x86_64.zip
   ```

1. Extrayez l'archive, créez le répertoire cible, puis déplacez-y `orbit.exe`. Cet exemple utilise `$env:LOCALAPPDATA\Programs\orbit`, la même valeur par défaut que celle utilisée par le programme d'installation :

   ```powershell
   Expand-Archive -Path .\orbit-local-windows-x86_64.zip -DestinationPath .
   New-Item -ItemType Directory -Force -Path "$env:LOCALAPPDATA\Programs\orbit"
   Move-Item .\orbit.exe "$env:LOCALAPPDATA\Programs\orbit\orbit.exe"
   ```

1. Si ce répertoire ne figure pas encore dans votre `PATH`, ajoutez-le pour votre utilisateur :

   ```powershell
   [Environment]::SetEnvironmentVariable("PATH", "$env:LOCALAPPDATA\Programs\orbit;$([Environment]::GetEnvironmentVariable('PATH', 'User'))", "User")
   ```

Aucun droit d'administrateur n'est requis.

Ouvrez un nouveau terminal, puis vérifiez :

```shell
orbit help
```

{{< /tab >}}

{{< tab title="npm" >}}

Vous pouvez également installer depuis npm, sur n'importe quelle plateforme :

```shell
npm install -g @gitlab/orbit
```

Le package [`@gitlab/orbit`](https://www.npmjs.com/package/@gitlab/orbit) installe le binaire prédéfini pour votre plateforme. Sur Linux, il utilise toujours le binaire musl entièrement statique, qui fonctionne sur les distributions glibc et musl.

Vérifiez :

```shell
orbit help
```

{{< /tab >}}

{{< tab title="CLI GitLab (glab)" >}}

Si vous avez déjà [`glab`](https://gitlab.com/gitlab-org/cli) installé :

```shell
glab orbit local --install
```

Vérifiez :

```shell
glab orbit local help
```

Consultez la [référence `glab orbit local`](https://docs.gitlab.com/cli/orbit/local/) pour plus de détails.

{{< /tab >}}

{{< /tabs >}}

## Choisir une méthode d'accès {#pick-an-access-method}

| Méthode | Idéale pour | Configuration |
|---|---|---|
| [Le CLI GitLab Orbit (`orbit`)](access/cli.md) | Utilisation directe du CLI, scripts, tâches d'indexation | Programme d'installation en une ligne ou `glab orbit local --install` |
| [Le CLI GitLab (`glab`)](access/glab.md) | Toute personne utilisant déjà `glab` | `glab orbit local --install` |
| [MCP](access/mcp.md) | Claude Code, Codex et autres agents d'IA | `claude mcp add orbit-local -- orbit mcp serve` |

Les trois méthodes lisent le même graphe local. GitLab Orbit Local est interrogé avec DuckDB SQL ; le DSL de requête JSON structuré est réservé à [GitLab Orbit Remote](../remote/_index.md) uniquement.

## Démarrage rapide en 60 secondes {#60-second-quickstart}

> [!note]
`glab orbit local` encapsule le binaire `orbit` géré. Le binaire est téléchargé, vérifié par somme de contrôle et maintenu à jour lors de la première utilisation. Nécessite `glab` 1.94 ou une version ultérieure. Pour exécuter le binaire directement à la place, consultez [Utiliser le CLI `orbit` directement](access/cli.md).

Indexez un dépôt et inspectez ce que GitLab Orbit a trouvé :

```shell
glab orbit local index /path/to/your/repo
glab orbit local schema
```

Cela crée un graphe DuckDB local dans `~/.orbit/graph.duckdb` et affiche chaque table et colonne qu'il contient : `gl_definition`, `gl_file`, `gl_directory`, `gl_imported_symbol`, `gl_edge`, et la table de gestion `_orbit_manifest`.

Ensuite :

- Exécutez une vraie requête : [Utiliser GitLab Orbit Local avec glab](access/glab.md).
- Intégrez-le à votre agent d'IA : exécutez `glab orbit setup` pour installer la compétence GitLab Orbit, ou [connectez-vous via MCP](access/mcp.md).
- Parcourez la disposition des tables : [Référence du schéma](schema.md).

## Facturation {#billing}

GitLab Orbit Local ne consomme pas de GitLab Credits. Tout le traitement est local.

## Que faire ensuite {#what-to-try-next}

- [Ce que GitLab Orbit Local indexe](indexing.md) \- langages et portée de couverture.
- [Référence du schéma](schema.md) \- les quatre types de nœuds dans le graphe local.
- [Cookbook](../remote/cookbook.md) \- requêtes à copier-coller (celles uniquement en code s'appliquent à Local).
- [Premiers pas avec GitLab Orbit Remote](../remote/getting-started.md) \- interrogez votre instance GitLab complète.
