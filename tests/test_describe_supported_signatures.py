from datacustomcode.deploy import describe_supported_signatures


def test_describe_supported_signatures(tmp_path):
    entrypoint = tmp_path / "entrypoint.py"
    chunk_pair = (
        "SearchIndexChunkingV1Request/SearchIndexChunkingV1Response "
        "for use_in_feature: SearchIndexChunking"
    )

    def describe(request_type, response_type):
        entrypoint.write_text(
            f"def function(request: {request_type}, runtime)"
            f" -> {response_type}:\n    pass\n"
        )
        return describe_supported_signatures(str(entrypoint))

    # A recognised request or response narrows the hint to its own feature
    assert describe("SearchIndexChunkingV1Request", "dict") == chunk_pair
    assert describe("dict", "SearchIndexChunkingV1Response") == chunk_pair
    # Unrecognised types fall back to listing every supported pair
    fallback = describe("dict", "dict")
    assert fallback == chunk_pair
