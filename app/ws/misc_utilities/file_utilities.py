import subprocess
from pathlib import Path
from flask import current_app as app


class FileUtils:

    @staticmethod
    def tree(dir_path: Path, prefix: str = ''):
        """A recursive generator, given a directory Path object
        will yield a visual tree structure line by line
        with each line prefixed by the same characters

        :param dir_path: Directory to print out recursively in tree form, as a Path object.
        :param prefix: Token string used to prefix any tree nodes.
        """

        # prefix components:
        space = '    '
        branch = '│   '
        # pointers:
        tee = '├── '
        last = '└── '

        contents = list(dir_path.iterdir())
        # contents each get pointers that are ├── with a final └── :
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, path in zip(pointers, contents):
            yield prefix + pointer + path.name + ' ' + FileUtils.render_filesize(path)
            if path.is_dir():  # extend the prefix and recurse:
                extension = branch if pointer == tee else space
                # i.e. space because last, └── , above so no more |
                yield from FileUtils.tree(path, prefix=prefix + extension)

    @staticmethod
    def ls(path):
        blacklist = app.config.get('STUDY_TREE_BLACKLIST')
        readout = subprocess.check_output(["ls", "-lR"], cwd=path).decode(
            "utf-8")
        return FileUtils.sanitise_ls_readout(readout, blacklist)


    @staticmethod
    def render_filesize(path: Path):
        """Render the size of the file in text IF it is a file and not a directory."""
        if path.is_dir():
            return ''
        else:
            return str(path.stat().st_size) + ' bytes'

    @staticmethod
    def sanitise_ls_readout(readout, blacklist):
        """Removes any information about our infrastructure.
        Creates a generator that spits out lines (to save memory in case of huge study folders), iterates over each line
        removing any blacklist words

        :param readout: The decoded results of the ls command.
        :param blacklist: List of values to scrub. Not hardcoded for obvious reasons.
        :return: Scrubbed version of ls output."""
        new_lines = []
        line_generator = (line for line in readout.split("\n"))
        for line in line_generator:
            tokens = line.split(" ")
            if len(tokens) > 2:
                new_lines.append(' '.join([token for token in tokens if token not in blacklist]))
            else:
                new_lines.append(line)
        return '\n'.join(new_lines)



