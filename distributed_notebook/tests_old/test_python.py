import pytest

class _T(tuple):
  pass

class TestPython:
  def test_buildin_redefinition(self):
    a = (1, 2, 3)
    b = _T(a)
    assert isinstance(a, tuple)
    assert not isinstance(a, _T)
    assert isinstance(b, _T)
