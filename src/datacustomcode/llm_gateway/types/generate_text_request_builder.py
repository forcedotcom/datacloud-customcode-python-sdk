from datacustomcode.llm_gateway.types.generate_text_request import GenerateTextRequest


class GenerateTextRequestBuilder:
    def __init__(self):
        self._prompt = ""
        self._model_name = ""
        self._localization = None
        self._tags = None

    def set_prompt(self, prompt: str):
        self._prompt = prompt
        return self

    def set_model(self, model_name: str):
        self._model_name = model_name
        return self

    def set_localization(self, localization: dict = None, locale: str = None):
        """
        Set localization either from a dict or a simple locale string.

        Args:
          localization: Full localization dict (if provided, locale is ignored)
          locale: Simple locale string for defaultLocale only

        Returns:
            self for method chaining
        """

        if localization is not None:
            self._localization = localization
        elif locale is not None:
            self._localization = {"defaultLocale": locale}
        else:
            raise ValueError("Must provide either localization or locale")

        self._localization = localization
        return self

    def set_tags(self, tags: dict):
        self._tags = tags
        return self

    def build(self) -> GenerateTextRequest:

        request = GenerateTextRequest(
            prompt=self._prompt,
            model_name=self._model_name,
            localization=self._localization,
            tags=self._tags
        )

        return request
