# Contributing

[README.md § Development](README.md#development) covers the toolchain and the
test and lint recipes. [AGENT.md](AGENT.md) holds the pre-push checklist and
the repository invariants.

## Adding a changeset

Releases are cut from changesets ([Sampo](https://github.com/bruits/sampo)):
a small markdown file in `.sampo/changesets/` that names what your PR
changes, how big the bump is, and what a user of the release should read.
Merging one to `main` starts the release flow (AGENT.md § Versioning); the
version strings in the tree are never edited by hand.

Two packages can be named:

| Package | What it covers | Bump |
|---|---|---|
| `hoglake` | the server, the trino harness, the webui, the OpenAPI spec, pyhoglake and hedgerow: one version, `vX.Y.Z` | `patch` for a fix, `minor` for a feature or a new route, `major` for a break |
| `pyhoglake-variant-codec` | pyhoglake's Rust codec, released on its own | any change to its source needs one (CI's `codec-version` requires it; a hand-bumped version is accepted instead and the next release tags it, but then it has no changelog entry) |

Write one with the CLI (`cargo install sampo`, or `npx sampo`), from the
repository root:

```bash
sampo add -p hoglake -b minor -t Server -m "Atomic partition replacement: ..."
```

or by hand, any file name, this shape:

```markdown
---
hoglake: minor (Server)
---

Atomic partition replacement: a new commit route ends live files and appends
their replacements in one snapshot. The changefeed records it as `table_replaced`.
```

The optional `(Tag)` picks the section of `CHANGELOG.md` the entry lands in
(`Server`, `webui`, `pyhoglake`, `hedgerow`, `millrace`, `Operators`,
`Dependencies`, `CI`; the list is `changesets.tags` in `.sampo/config.toml`).
Write the body for the reader of the release notes: what changed, what they
must do (an `Operators` entry for a migration or a default that moved), and
the PR number. One PR may carry several changesets, and one changeset may
name both packages. A refactor, a test, a doc or a CI change needs none; the
`Changeset hygiene` comment on your PR is a reminder, not a gate.

A codec changeset normally travels with a `hoglake: patch (pyhoglake)` one
saying that pyhoglake's `variant-codec` extra now pins the new codec, so
the pin ships in a pyhoglake release.

## Signed commits

`main` accepts only commits with a verified signature, and a pull request with
an unsigned commit can't merge. The `signed-commits` check fails on such a
pull request and lists the commits to fix, with the commands below, in its
failure message and job summary.

### One-time setup (SSH signing)

SSH signing needs git 2.34 or newer. Point `user.signingkey` at your public
key; `id_ed25519.pub` below is an example, and an RSA key (`id_rsa.pub`) works
too. If you have no key, create one with `ssh-keygen -t ed25519`.

```bash
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
```

Drop `--global` (and run the commands inside your clone) to sign in this
repository only.

Then add the same public key on GitHub under **Settings → SSH and GPG keys →
New SSH key**, with **Key type: Signing Key**. A key added only for
authentication doesn't verify commits, even when it's the same key.

Your `git config user.email` must be a verified email on your GitHub account,
or GitHub shows the commit as unverified even though it's signed. It is the
committer email GitHub checks.

GPG signing works too, and is the route on older git; see GitHub's
[commit signature verification](https://docs.github.com/en/authentication/managing-commit-signature-verification)
docs.

### Fixing an open pull request

Re-sign every commit on the branch onto the current `main`, then force-push:

```bash
git fetch https://github.com/PostHog/hoglake.git main
git rebase --exec 'git commit --amend --no-edit -S' FETCH_HEAD
git push --force-with-lease
```

The amend keeps each commit's author and message, `Co-authored-by` trailers
included, and makes you the signing committer, so it also fixes commits
someone else authored.

Rebasing drops any "Update branch" merge commits GitHub added. If one of them
resolved a conflict, the rebase stops there: resolve it again, `git add` the
files and `git rebase --continue`, or `git rebase --abort` to start over.
