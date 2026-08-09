package ac.onyx.docstack.store

public class InMemoryDocumentStoreTest : DocumentStoreConformanceTest() {
    override fun createStore(): DocumentStore = InMemoryDocumentStore()
}
