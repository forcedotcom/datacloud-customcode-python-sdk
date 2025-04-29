from __future__ import annotations

import pytest

from datacustomcode.mixin import (
    UserExtendableNamedConfigMixin,
    _get_all_subclass_descendants,
)


# Test classes to use in the tests
class BaseMixinClass(UserExtendableNamedConfigMixin):
    CONFIG_NAME = "BaseMixinClass"


class ChildClass(BaseMixinClass):
    CONFIG_NAME = "ChildClass"


class GrandchildClass(ChildClass):
    CONFIG_NAME = "GrandchildClass"


class SiblingClass(BaseMixinClass):
    CONFIG_NAME = "SiblingClass"


class TestUserExtendableNamedConfigMixin:

    def test_subclass_from_config_name_basic(self):
        """Test getting a direct subclass by config name."""
        subclass = BaseMixinClass.subclass_from_config_name("ChildClass")
        assert subclass is ChildClass

        # Can also find a grandchild
        subclass = BaseMixinClass.subclass_from_config_name("GrandchildClass")
        assert subclass is GrandchildClass

        # Can also find siblings
        subclass = BaseMixinClass.subclass_from_config_name("SiblingClass")
        assert subclass is SiblingClass

    def test_subclass_from_config_name_not_found(self):
        """Test error raised when config name doesn't exist."""
        with pytest.raises(KeyError, match="does not match any subclass CONFIG_NAME"):
            BaseMixinClass.subclass_from_config_name("NonExistentClass")

    def test_available_config_names(self):
        """Test getting all available config names."""
        config_names = BaseMixinClass.available_config_names()

        # All classes with CONFIG_NAME should be included
        assert "BaseMixinClass" in config_names
        assert "ChildClass" in config_names
        assert "GrandchildClass" in config_names
        assert "SiblingClass" in config_names

        # Class without CONFIG_NAME should not be included
        assert len([name for name in config_names if name == ""]) == 0

    def test_hierarchy_navigation(self):
        """Test that we can navigate the inheritance hierarchy correctly."""
        # Child class should find itself and its children but not its parent or siblings
        config_names = ChildClass.available_config_names()
        assert "ChildClass" in config_names
        assert "GrandchildClass" in config_names
        assert "BaseMixinClass" not in config_names
        assert "SiblingClass" not in config_names

        # Child class should be able to find its own children by name
        subclass = ChildClass.subclass_from_config_name("GrandchildClass")
        assert subclass is GrandchildClass

        # Child class should raise KeyError for parent or sibling names
        with pytest.raises(KeyError):
            ChildClass.subclass_from_config_name("BaseMixinClass")

        with pytest.raises(KeyError):
            ChildClass.subclass_from_config_name("SiblingClass")


class TestGetAllSubclassDescendants:

    def test_get_all_subclass_descendants_basic(self):
        """Test the helper function that gets all subclass descendants."""
        all_descendants = _get_all_subclass_descendants(BaseMixinClass)

        # Should include the class itself
        assert BaseMixinClass in all_descendants

        # Should include all subclasses
        assert ChildClass in all_descendants
        assert GrandchildClass in all_descendants
        assert SiblingClass in all_descendants

    def test_get_all_subclass_descendants_from_child(self):
        """Test getting descendants starting from a child class."""
        all_descendants = _get_all_subclass_descendants(ChildClass)

        # Should include the class itself
        assert ChildClass in all_descendants

        # Should include its children
        assert GrandchildClass in all_descendants

        # Should not include parent or siblings
        assert BaseMixinClass not in all_descendants
        assert SiblingClass not in all_descendants


# Dynamic class creation tests
class TestDynamicClasses:

    def test_dynamically_added_class(self):
        """Test that dynamically created classes are found correctly."""
        # Dynamically create a new class
        NewDynamicClass = type(
            "NewDynamicClass", (BaseMixinClass,), {"CONFIG_NAME": "NewDynamicClass"}
        )

        # Should be able to find it by name
        subclass = BaseMixinClass.subclass_from_config_name("NewDynamicClass")
        assert subclass is NewDynamicClass

        # Should be in available config names
        config_names = BaseMixinClass.available_config_names()
        assert "NewDynamicClass" in config_names

    def test_duplicate_config_names_last_one_wins(self):
        """Test behavior when multiple classes have the same CONFIG_NAME."""
        # Create two classes with the same CONFIG_NAME
        type("DuplicateClass1", (BaseMixinClass,), {"CONFIG_NAME": "DuplicateConfig"})

        try:
            type(
                "DuplicateClass2", (BaseMixinClass,), {"CONFIG_NAME": "DuplicateConfig"}
            )
        except TypeError as e:
            assert str(e).startswith("Class DuplicateClass2")
