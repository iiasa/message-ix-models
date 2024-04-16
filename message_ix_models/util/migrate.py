from collections import defaultdict
from dataclasses import dataclass
from hashlib import blake2s
from pathlib import Path
from typing import Literal

import click
import git


@dataclass
class RepoInfo:
    url: str
    branch: str

    @property
    def name(self) -> str:
        return self.url.split("/")[-1].split(".")[0]

    @property
    def base(self) -> str:
        return self.name.replace("-", "_")


# Path fragment for path renaming and filtering
MOD = "doc"

S = SOURCE = RepoInfo(
    url="git@github.com:iiasa/message_data.git",
    branch="dev",
)

S = SOURCE = RepoInfo(
    url="git@github.com:iiasa/message_doc.git",
    branch="main",
)


D = DEST = RepoInfo(
    url="git@github.com:iiasa/message-ix-models.git",
    branch=f"migrate-{MOD}",
)


# Batches of commands for git-filter-repo
BATCH = {
    # --path-rename = rename several paths and files under them, i.e.:
    #   - Module data.
    #   - Module code. The "/model/" path fragment could also be "/project/", or removed
    #     entirely.
    #   - Module tests.
    0: (
        # Add or remove lines here as necessary; not all modules have all the following
        # pieces, and some modules have additional pieces.
        f"--path-rename=data/{MOD}/:{D.base}/data/{MOD}/",
        f"--path-rename={S.base}/model/{MOD}/:{D.base}/model/{MOD}/",
        f"--path-rename={S.base}/tests/model/{MOD}/:{D.base}/tests/model/{MOD}/",
    ),
    #
    # --path = keep only a subset of files and directories.
    #
    # This has the effect of discarding the top-level message_data and data directories,
    # keeping only message_ix_models. This operates on the paths renamed by the previous
    # command. It would be possible to combine in a single command, but we would then
    # need to specify the *original* paths to keep.
    1: [
        "--message-callback="
        """'return re.sub(b"^[a-z]", message[:1].upper(), message)'""",
        f"--path={D.base}",
        # NB can add lines to keep other files, for instance:
        # --path doc/$MOD/
    ],
    #
    # --invert-paths = *remove* some specific files, e.g. non-reporting test data
    2: [
        # commented: currently not used
        # "--invert-paths",
        # f"--path-regex=^{d}/tests/data/[^r].*$",
    ],
}


@click.group
@click.pass_context
def main(ctx):
    ctx.ensure_object(dict)

    for label, info in ("source", SOURCE), ("dest", DEST):
        # Local directory containing a git repo
        hash_ = blake2s(info.url.encode()).hexdigest()[:3]
        repo = git.Repo.init(Path.cwd().joinpath(f"{label}-{hash_}"))
        from icecream import ic

        ic(repo, repo.working_dir)

        # Store for usage in commands
        ctx.obj.update({f"{label} info": info, f"{label} repo": repo})


def delete_remote(repo: "git.Repo", name: str = "origin"):
    """Delete the remote with the given `name` from `repo`, if it exists."""
    try:
        repo.remotes[name]
    except IndexError:
        pass
    else:
        repo.delete_remote(name)


def find_initial_commit(repo: "git.Repo") -> "git.Commit":
    return repo.commit(repo.git.rev_list("--max-parents=0", "HEAD"))


def mirror_lfs_objects(src: "git.Repo", dest: "git.Repo") -> None:
    """Symlink Git LFS objects in `src` from `dest`."""
    s_base = Path(src.git_dir, "lfs", "objects")
    d_base = Path(dest.git_dir, "lfs", "objects")

    for s_path in s_base.rglob("*"):
        d_path = d_base.joinpath(s_path.relative_to(s_base))

        if s_path.is_dir():
            d_path.mkdir(parents=True, exist_ok=True)
            continue

        try:
            d_path.symlink_to(s_path)
        except FileExistsError:
            if not (
                (d_path.is_symlink() and d_path.readlink() == s_path)
                or d_path.stat().st_size == s_path.stat().st_size
            ):
                raise FileExistsError(
                    f"{d_path}, but is not the expected symlink to/copy of {s_path}"
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
    label=Literal["source", "dest"],
    branch: bool = False,
    lfs: bool = False,
) -> None:
    repo: "git.Repo" = config[f"{label} repo"]
    info: RepoInfo = config[f"{label} info"]

    # Ensure there is a remote for the origin
    delete_remote(repo, "origin")
    origin = repo.create_remote("origin", info.url)

    # Fetch the remote
    name = info.branch if branch else "main"
    origin.fetch(f"refs/heads/{name}")
    b = origin.refs[name]

    # Check out the branch
    try:
        head = repo.heads[name]
    except KeyError:
        head = repo.create_head(name, b)
    head.set_tracking_branch(b).checkout(force=True)

    # Fetch all LFS objects existing on the server; about 5.7 GB for message_data
    if lfs:
        repo.git.lfs("fetch", "--all")

    # Delete the `origin` remote to avoid inadvertent pushing
    delete_remote(repo, "origin")

    return repo


@main.command
@click.pass_obj
def prep(config):
    src = prep_repo(config, "source", branch=True, lfs=True)

    # Write a list of missing LFS objects
    to_remove = missing_lfs_objects(src)
    Path.cwd().joinpath("lfs-sha256s-to-remove.txt").write_text("\n".join(to_remove))

    dest = prep_repo(config, "dest")

    # Symlink LFS objects from source repo's .git directory.
    #
    # This ensures that when commit history is being rewritten in the current directory,
    # references to LFS objects are resolved, because they are in the local cache.
    mirror_lfs_objects(src, dest)


@main.command
@click.pass_obj
def migrate(config):
    # Source and destination repos
    src: "git.Repo" = config["source repo"]
    dest: "git.Repo" = config["dest repo"]

    # Configuration
    s_info: RepoInfo = config["source info"]
    # d_info: RepoInfo = config["dest info"]

    # Record the initial commit of the `dest` "main" branch
    dest_initial_commit = find_initial_commit(dest, "main")
    print(f"Will rebase initial commits on to: {dest_initial_commit!r}")

    # Add a remote to `dest` that points to `src`
    delete_remote(dest, "source-remote")
    src_remote = dest.create_remote("source-remote", src.working_tree_dir)

    # Fetch (local)
    src_remote.fetch()

    # Create a branch in the `dest` that tracks a branch in `src`
    try:
        head = dest.heads["source-branch"]
    except IndexError:
        head = dest.create_head("source-branch", src_remote.refs[s_info.branch])
    finally:
        head.set_tracking_branch(src_remote.refs[s_info.branch])

    # Common args for calls to filter-repo in `dest`
    common = [f"--refs={head}", "--force", "--debug"]

    # Run each batch of commands
    for i, args in BATCH.items():
        if not args:
            print("No actions for batch {i}; skip")
            continue

        dest.git.filter_repo(*common, *args)

    # Filtering is complete

    src_initial_commit = find_initial_commit(dest, head)

    # Graft the initial commit(s) of the rewritten branch onto the initial commit of
    # `dest`, so the two share a common initial commit

    dest.git.replace("--graft", src_initial_commit.hexsha, dest_initial_commit.hexsha)

    # FIXME If there are more than one, the git-replace operations must be done together
    # and filter-repo last.

    # Run filter repo one more time to rewrite
    dest.git.filter_repo(*common)

    # Log of the rewritten branch
    todo_lines = list()
    messages = defaultdict(list)

    for commit in dest.iter_commits(head):
        todo_lines.append(f"p {commit.hexsha[:8]}\n")
        message = commit.message.splitlines()[0]
        messages[message].append(f"{commit.hexsha[:8]}  {message}")

    # Generate a TODO list for "git rebase --interactive --empty=drop main"
    with open("rebase-todo.in", "w") as f:
        f.write("""# This is a candidate TODO list for:
#   git rebase --interactive --empty=drop main

""")
        f.writelines(reversed(todo_lines))

    # Generate a list to help identifying duplicate commits
    with open("duplicate-messages.txt", "w") as f:
        f.write("""# This is a list of commits with duplicate messages""")
        for k, v in messages.items():
            if len(v) == 1:
                continue
            f.write("\n\n" + "\n".join(reversed(v)))


@main.command
@click.pass_obj
def reset(config):
    # Checks out the `main` branch, wiping out any uncommitted changes
    dest = prep_repo(config, "dest")

    # Remove the remote pointing to migrate-src
    delete_remote(dest, "migrate-src")

    # Remove the target branch
    try:
        dest.heads[config["dest branch"]]
    except IndexError:
        pass
    else:
        dest.delete_head(config["dest branch"], force=True)

    # Remove generated files
    for name in "lfs-sha256s-to-remove.txt", "rebase-todo.in", "duplicate-messages.txt":
        Path.cwd().joinpath(name).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
