class LLMGateway(BaseLLMGateway):
    def generate_text(
            self,
            request: GenerateTextRequest
    ) -> GenerateTextResponse:


        response_data = {
            'generation' : {'generatedText' : "I am dreaming!!"},
        }

        return GenerateTextResponse(200, {"data": response_data})