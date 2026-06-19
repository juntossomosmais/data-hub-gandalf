class DuplicatedDataFrameError(Exception):
    """Raised when the input DataFrame has duplicated rows."""

    output_message = "The input dataframe has duplicated rows\n  Duplicated rows:\n  "

    def __init__(self, dup_rows, message=output_message):
        self.message = message
        super().__init__(self.message + dup_rows)


class InvalidColumnsError(Exception):
    """Raised when the source columns do not match the target columns."""

    def __init__(
        self,
        divergence_list,
        message="The input columns must match the target columns, please fix it before merging",
    ):
        self.message = message
        super().__init__(self.message + divergence_list)


class RowCountException(Exception):
    """Raised when source row count does not match the target after merge."""

    def __init__(
        self,
        divergence,
        message="Source row count must match target row count after merge",
    ):
        self.message = message
        super().__init__(self.message + divergence)


class InvalidTable(Exception):
    """Raised when the destination path is not a valid Delta table."""

    def __init__(self, message="The destination path does not point to a Delta table"):
        self.message = message
        super().__init__(self.message)
