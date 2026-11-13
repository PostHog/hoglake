# Contributing

[README.md § Development](README.md#development) covers the toolchain and the
test and lint recipes. [AGENT.md](AGENT.md) holds the pre-push checklist and
the repository invariants.

## Signed commits

`main` accepts only commits with a verified signature, and a pull request with
an unsigned commit can't merge. The `signed-commits` check fails on such a
pull request and comments with the commits to fix.

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
