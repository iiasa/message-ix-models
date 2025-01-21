import os
from collections import defaultdict
from dataclasses import dataclass
from hashlib import blake2s
from pathlib import Path
from typing import Literal

import click
import git
from git_filter_repo import FilteringOptions, RepoFilter

#: Common args for calls to git-filter-repo in `target_repo`
COMMON = [
    "--force",  # Ignore non-clean history
    # "--debug",  # Show debug information
    "--prune-empty=always",
    "--prune-degenerate=always",
]


@dataclass
class RepoInfo:
    #: Source (probably GitHub) URL of the remote to clone.
    url: str

    #: Branch to use.
    branch: str = "main"

    @property
    def base(self) -> str:
        """Likely top-level directory in the repo, a Python module name."""
        return self.url.split("/")[-1].split(".")[0].replace("-", "_")


def message_callback(message: bytes) -> bytes:
    return message.capitalize()


# -- Configuration ---------------------------------------------------------------------
# Do not edit outside of this section. See the documentation for examples.

S = SOURCE = RepoInfo(
    url="git@github.com:iiasa/message_doc.git",
    branch="main",
)

T = TARGET = RepoInfo(
    url="git@github.com:iiasa/message-ix-models.git",
    branch="main",
)


BATCH = (
    dict(
        args=[
            "--path-rename=:doc/global/",
            "--path-rename=doc/global/_static/:doc/_static/",
            "--replace-message=../replacements.txt",
        ],
        message_callback=message_callback,
    ),
    dict(
        args=["--invert-paths", "--path=doc/_static/combined-logo-white.png"],
    ),
)

# --------------------------------------------------------------------------------------


def delete_remote(repo: "git.Repo", name: str = "origin"):
    """Delete the remote with the given `name` from `repo`, if it exists."""
    try:
        repo.remotes[name]
    except IndexError:
        pass
    else:
        repo.delete_remote(name)


def find_initial_commit(repo: "git.Repo", name: str) -> "git.Commit":
    return repo.commit(repo.git.rev_list("--max-parents=0", name))


def mirror_lfs_objects(source_repo: "git.Repo", target_repo: "git.Repo") -> None:
    """Symlink Git LFS objects in `source_repo` from `target_repo`."""
    s_base = Path(source_repo.git_dir, "lfs", "objects")
    t_base = Path(target_repo.git_dir, "lfs", "objects")

    for s_path in s_base.rglob("*"):
        t_path = t_base.joinpath(s_path.relative_to(s_base))

        if s_path.is_dir():
            t_path.mkdir(parents=True, exist_ok=True)
            continue

        try:
            t_path.symlink_to(s_path)
        except FileExistsError:
            if not (
                (t_path.is_symlink() and t_path.readlink() == s_path)
                or t_path.stat().st_size == s_path.stat().st_size
            ):
                raise FileExistsError(
                    f"{t_path}, but is not the expected symlink to/copy of {s_path}"
                )


def missing_lfs_objects(repo: "git.Repo") -> list["Path"]:
    """Return a list of missing LFS objects in `repo`; possibly empty.

    Modified from https://github.com/git-lfs/git-lfs/issues/4232#issuecomment-689967255.
    """
    base = Path(repo.git_dir, "lfs", "objects")

    missing = set()

    for line in repo.git.lfs("ls-files", "--all", "--long").splitlines():
        try:
            hash_, sep, path = line.split(maxsplit=2)
        except ValueError:
            continue

        p = base.joinpath(hash_[:2], hash_[2:4], hash_)
        if not p.exists():
            missing.add(hash_)

    return sorted(missing)


def prep_repo(
    config: dict,
    label=Literal["source", "target"],
    lfs: bool = False,
) -> None:
    """Prepare the git repo with the given `label`.

    - Add the remote 'origin'.
    - Fetch the configured branch from 'origin'.
    - Check out the branch.
    - Fetch all LFS objects existing on the server.
    - Delete the 'origin' remote, to avoid inadvertent pushes.
    """
    repo: "git.Repo" = config[f"{label} repo"]
    info: RepoInfo = config[f"{label} info"]

    # Ensure there is a remote for the origin
    delete_remote(repo, "origin")
    origin = repo.create_remote("origin", info.url)

    # Fetch the remote
    branch_name = info.branch
    origin.fetch(f"refs/heads/{branch_name}")
    b = origin.refs[branch_name]

    # Check out the branch
    try:
        head = repo.heads[branch_name]
    except IndexError:
        head = repo.create_head(branch_name, b)
    head.set_tracking_branch(b).checkout(force=True)

    if label == "source":
        # Fetch all LFS objects existing on the server
        repo.git.lfs("fetch", "--all")

        # Write a list of missing LFS objects
        to_remove = missing_lfs_objects(repo)
        Path.cwd().joinpath("lfs-sha256s-to-remove.txt").write_text(
            "\n".join(map(str, to_remove))
        )

    # Delete the `origin` remote
    delete_remote(repo, "origin")

    return repo


def same_commit(prev: "git.Commit", commit: "git.Commit") -> bool:
    """Return True if `prev` and `commit` have the same message and author name.

    This allows for the e-mail address to be different, e.g. contain typos.
    """
    return prev.message == commit.message and prev.author.name == commit.author.name


@click.group
@click.pass_context
def main(ctx):
    ctx.ensure_object(dict)
    config = ctx.obj

    for label, info in ("source", SOURCE), ("target", TARGET):
        # Local directory containing a git repo
        hash_ = blake2s(repr(info).encode()).hexdigest()[:3]
        repo = git.Repo.init(Path.cwd().joinpath(f"{label}-{hash_}"))

        print(f"{label} remote : {info.url}:{info.branch}")
        print(f"       local  : {repo.working_dir}")

        # Store for usage in commands
        config.update({f"{label} info": info, f"{label} repo": repo})

    print()


@main.command("step-1")
def step_1():
    """Copy the script into the temporary directory."""
    wd = Path.cwd()
    file_path = Path(__file__)
    script_path = wd.joinpath(file_path.name)

    if not script_path.exists():
        import shutil
        import sys

        shutil.copyfile(file_path, script_path)

    print(f"Continue using '{sys.executable} {script_path.name}'")


@main.command("step-2")
@click.pass_obj
def step_2(config):
    """Prepare the source and target repos."""
    source_repo = prep_repo(config, "source", lfs=True)
    target_repo = prep_repo(config, "target")

    # Symlink LFS objects from source repo's .git directory.
    #
    # This ensures that when commit history is being rewritten in the current directory,
    # references to LFS objects are resolved, because they are in the local cache.
    mirror_lfs_objects(source_repo, target_repo)


@main.command("step-3")
@click.pass_obj
def step_3(config) -> None:
    """Rewrite history and prepare rebase."""
    cwd = Path.cwd()

    # Retrieve objects from configuration
    source_repo: "git.Repo" = config["source repo"]
    target_repo: "git.Repo" = config["target repo"]
    source_info: RepoInfo = config["source info"]

    # Record the initial commit of the `target_repo` "main" branch
    target_initial_commit = find_initial_commit(target_repo, "main")
    # print(f"Will rebase initial commits on to: {target_initial_commit!r}")
    del target_initial_commit  # Currently unused

    # Add a remote to `target_repo` that points to `source_repo`
    delete_remote(target_repo, "source-remote")
    source_remote = target_repo.create_remote("source-remote", source_repo.working_dir)

    # Fetch (local)
    source_remote.fetch()
    b = source_remote.refs[source_info.branch]

    # Create a branch in the `target_repo` that tracks a branch in `source_repo`
    # NB For some reason the same approach as in prep_repo() does not work here
    target_repo.git.checkout(f"remotes/{b.name}", b="source-branch")
    head = target_repo.heads["source-branch"]
    # Only rewrite the branch `head`
    COMMON.append(f"--refs={head}")

    # Remove the remote again
    target_repo.delete_remote(source_remote)

    # Change to the target repo working tree for git-filter-repo
    os.chdir(target_repo.working_tree_dir)

    # Run each batch of commands
    for i, config in enumerate(BATCH):
        # Parse CLI-ish arguments: common arguments and batch-specific
        fo = FilteringOptions.parse_args(COMMON + config.pop("args"))
        # Create and run a RepoFilter using parsed arguments and any other kwargs
        RepoFilter(fo, **config).run()

    # NB the following block is commented because the migration process currently uses
    #    an interactive rebase in step (5).
    #
    # # Identify the initial commit of the rewritten source-branch
    # source_initial_commit = find_initial_commit(target_repo, head)
    #
    # # Graft the initial commit(s) of the rewritten branch onto the initial commit of
    # # `target_repo`, so the two share a common initial commit
    # target_repo.git.replace(
    #     "--graft", source_initial_commit.hexsha, target_initial_commit.hexsha
    # )
    #
    # # Run filter repo one more time to rewrite
    # target_repo.git.filter_repo(*common)

    # Log of the rewritten branch
    todo_lines = list()
    # Non-adjacent, identical messages
    messages = defaultdict(list)
    # Info on the previous commit
    prev = None

    # Get a list of the commits from the initial to latest
    commits = reversed(list(target_repo.iter_commits(head)))

    for c in commits:
        first_line = c.message.splitlines()[0]

        if prev and same_commit(prev, c):
            # Same as previous commit's message *and* author → suggest to squash while
            # keeping only the first message
            action = "fixup"
            # Don't add to messages
        else:
            action = "pick"
            # Record the hash
            messages[first_line].append(f"{c.hexsha[:8]}  {first_line}")

        # Add to TODO lines
        todo_lines.append(f"{action} {c.hexsha[:8]}  {first_line}\n")

        # Update for next commit
        prev = c

    # Generate a TODO list for "git rebase --interactive --empty=drop main"
    with open(cwd.joinpath("rebase-todo.in"), "w") as f:
        f.write("""# This is a candidate TODO list for:
#   git rebase --interactive --empty=drop main

""")
        f.writelines(todo_lines)

    # Generate a list to help identifying duplicate commits
    with open(cwd.joinpath("duplicate-messages.txt"), "w") as f:
        f.write("""# This is a list of commits with duplicate messages""")
        for k, v in messages.items():
            if len(v) == 1:
                continue
            f.write("\n\n" + "\n".join(v))


@main.command
@click.pass_obj
def reset(config):
    """Undo changes."""
    # Checks out the `main` branch, wiping out any uncommitted changes
    target_repo = prep_repo(config, "target")

    # Remove the remote pointing to the source repo
    delete_remote(target_repo, "source-remote")

    # Remove the rewritten branch
    try:
        target_repo.heads["source-branch"]
    except IndexError:
        pass
    else:
        target_repo.delete_head("source-branch", force=True)

    # Remove generated files
    for name in "lfs-sha256s-to-remove.txt", "rebase-todo.in", "duplicate-messages.txt":
        Path.cwd().joinpath(name).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
