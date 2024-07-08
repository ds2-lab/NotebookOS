class Election(object):
    """
    Encapsulates the information about the current election term.
    """
    def __init__(
            self, 
            term_number: int
    ):
        self._term_number = term_number

    @property 
    def term_number(self)->int:
        """
        Return the term of this election.
        """
        return self._term_number 