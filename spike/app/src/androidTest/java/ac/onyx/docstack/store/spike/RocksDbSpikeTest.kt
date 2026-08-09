// Boot/viability spike only (spec 02 task 1): does RocksDB-on-Android support
// atomic WriteBatch and ordered iteration, and what's the open+write+scan cost?
// Not production code.
package ac.onyx.docstack.store.spike

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions
import java.io.File

@RunWith(AndroidJUnit4::class)
class RocksDbSpikeTest {

    private fun freshDbPath(name: String): String {
        val dir = InstrumentationRegistry.getInstrumentation().targetContext.cacheDir
        return File(dir, name).absolutePath
    }

    @Test
    fun writeBatchIsAtomicAndOrderedIterationWorks() {
        RocksDB.loadLibrary()
        val path = freshDbPath("spike-writebatch")
        val options = Options().setCreateIfMissing(true)

        val openStart = System.nanoTime()
        val db = RocksDB.open(options, path)
        val openMs = (System.nanoTime() - openStart) / 1_000_000

        try {
            // Atomic WriteBatch: several puts across different keys, one commit.
            val batch = WriteBatch()
            val keys = listOf("doc:001", "doc:002", "doc:003", "doc:004", "doc:005")
            for (key in keys) {
                batch.put(key.toByteArray(), "{\"hello\":\"$key\"}".toByteArray())
            }

            val writeStart = System.nanoTime()
            db.write(WriteOptions(), batch)
            val writeMs = (System.nanoTime() - writeStart) / 1_000_000

            // Observed atomicity: every key from the batch is present after commit.
            for (key in keys) {
                val value = db.get(key.toByteArray())
                assertTrue("expected $key to be present after WriteBatch commit", value != null)
                assertEquals("{\"hello\":\"$key\"}", String(value!!))
            }

            // Ordered iteration (snapshot iteration analogue for allDocs/changes):
            // keys come back in lexical order, matching ADR-0001's byte-ordering
            // assumption.
            val scanStart = System.nanoTime()
            val iterator = db.newIterator()
            val seen = mutableListOf<String>()
            iterator.seekToFirst()
            while (iterator.isValid) {
                seen.add(String(iterator.key()))
                iterator.next()
            }
            iterator.close()
            val scanMs = (System.nanoTime() - scanStart) / 1_000_000

            assertEquals(keys.sorted(), seen)

            println(
                "SPIKE_TIMING openMs=$openMs writeBatchMs=$writeMs scanMs=$scanMs " +
                    "keys=${keys.size}",
            )
        } finally {
            db.close()
        }
    }
}
