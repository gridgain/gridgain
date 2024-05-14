# Vector POC

In this example, OpenAI's Wikipedia corpus (25k documents) are indexed along with title and content vectors. 

## Getting Started

* Clone https://github.com/ggprivate/ggprivate/tree/gg-39175
* Download test dataset

    wget -P ggprivate/modules/lucene/src/test/resources https://cdn.openai.com/API/examples/data/vector_database_wikipedia_articles_embedded.zip

* Launch test GridCacheVectorQuerySelfTest.testVectorQueryWithField
* To change query please replace vector in `ggprivate/modules/lucene/src/test/resources/query.txt`. Vector can be retrived in OpenAI UI.

Test implementation was copied from this article https://searchscale.com/blog/vector-search-with-lucene/

NOTE:
* License with feature can be found here `ggprivate/modules/core/src/test/config/license/license-vector-search.xml`
* QueryVectorField annotation or QueryIndex#type = QueryIndexType.VECTOR should be used to identify the field to be indexed using a vector index.
* User can create a plugin controlled by him and implement TranslatorFactory extention to embedded string to vector.
