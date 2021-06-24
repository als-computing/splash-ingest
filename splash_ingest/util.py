from typing import List

from .model import Issue, Severity


class IssueCollectorMixin():
    _issues:List[Issue] = []
    def __init__(self, **kwargs) -> None:
        self.stage = kwargs.get('stage') if kwargs.get('stage') else 'unknown'

    @property
    def issues(self):
        return self._issues

    def add_warning(self, message, exception=None):
        self._issues.append(Issue(
            stage=self.stage, 
            severity=Severity.warning,
            msg=message, 
            exception=IssueCollectorMixin.serialize_execption(exception)))

    def add_error(self, message, exception=None):
        self._issues.append(Issue(
            stage=self.stage, 
            severity=Severity.error, 
            msg=message, 
            exception=IssueCollectorMixin.serialize_execption(exception)))

    @staticmethod
    def serialize_execption(exception: Exception) -> str:
        return repr(exception)