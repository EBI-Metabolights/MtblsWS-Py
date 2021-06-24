import logging

logger = logging.getLogger('wslog')


class StudyUtils:
    """A collection of utility methods common to the study resource and study service."""

    @staticmethod
    def is_template_file(filename: str) -> bool:
        """
        Assesses whether a file is a template file to be preserved.
        :return: bool indicating whether it's a template file.
        """
        return filename.upper().startswith("I_INVESTIGATION") or filename.upper().startswith("S_MTBLS")

    @staticmethod
    def overwrite_investigation_file(study_location: str, study_id: str) :
        """
        Overwrite the corresponding lines for given keys in an investigation file.
        Opens the investigation text file, finds the corresponding lines for the keys supplied, and overwrites
        those lines with new values.

        :param study_location: The filesystem location of the investigation file.
        :param study_id: Then accession number of the study.
        """

        try:
            i_file = open(study_location + '/i_Investigation.txt', 'r')
            lines = i_file.readlines()

            # Considered having the indexes of the two lines we want be static, retrieved from config.
            # But if there is any chance the lines are out of expected order it would be a pain.
            line_changes = {'Study Identifier': {
                'index': -1,
                'overwriting line': 'Study Identifier\t {0}\n'.format(study_id)
            }, 'Study File Name': {
                'index': -1,
                'overwriting line': 'Study File Name\t s_{0}.txt\n'.format(study_id)
            }}
            index = 0

            for line in lines:
                if line.lstrip().startswith('Study File Name'):
                    line_changes['Study File Name']['index'] = index
                if line.lstrip().startswith('Study Identifier'):
                    line_changes['Study Identifier']['index'] = index
                if line_changes['Study Identifier']['index'] > -1 and line_changes['Study File Name']['index'] > -1:
                    break
                index += 1

            # update the list of lines
            if line_changes['Study Identifier']['index'] > -1 and line_changes['Study File Name']['index'] > -1:
                for line, obj in line_changes.items():
                    lines[obj['index']] = obj['overwriting line']
            else:
                logger.error('Index of required line not found in investigation.txt file.')
                raise IndexError

            # overwrite the file, and then close it.
            i_file = open(study_location + '/i_Investigation.txt', 'w')
            i_file.writelines(lines)
            i_file.close()


        except OSError as e:
            logger.error("Investigation file could not be opened. Check that the investigation file is present"
                         " in the study folder: {0}".format(e))
            raise
        except Exception as e:
            logger.error("An unexpected error occurred: {0}".format(e))
            raise

        return True
