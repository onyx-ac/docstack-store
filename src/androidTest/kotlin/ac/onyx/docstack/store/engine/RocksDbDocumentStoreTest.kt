package ac.onyx.docstack.store.engine

import ac.onyx.docstack.store.DocumentStore
import ac.onyx.docstack.store.DocumentStoreConformanceTest
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import java.io.File
import java.util.UUID
import org.junit.runner.RunWith

/**
 * Runs the exact same [DocumentStoreConformanceTest] suite
 * [ac.onyx.docstack.store.InMemoryDocumentStoreTest] does, against
 * [RocksDbDocumentStore] instead — on-device only, since RocksDB's native `.so` only
 * loads on Android (spec 02 task 4).
 */
@RunWith(AndroidJUnit4::class)
public class RocksDbDocumentStoreTest : DocumentStoreConformanceTest() {

    override fun createStore(): DocumentStore {
        val cacheDir = InstrumentationRegistry.getInstrumentation().targetContext.cacheDir
        val dir = File(cacheDir, "rocksdb-store-test-${UUID.randomUUID()}")
        return RocksDbDocumentStore(dir)
    }
}
