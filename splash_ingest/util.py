from .model import Issue, Severity


class IssueCollectorMixin():
    _issues:list[Issue] = []
    def __init__(self, **kwargs) -> None:
        self.stage = kwargs.get('stage') if kwargs.get('stage') else 'unknown'

    @property
    def issues(self):
        return self._issues

    def add_warning(self, message, exception=None):
        self._issues.append(Issue(stage=self.stage, severity=Severity.warning, msg=message, exception=exception))

    def add_error(self, message, exception=None):
        self._issues.append(Issue(stage=self.stage, severity=Severity.error, msg=message, exception=exception))