# RocksDB-on-Android engine spike findings — spec 02 task 1

Status: **pass**. `io.maryk.rocksdb:rocksdb-android` is a real, current, drop-in
API-compatible RocksDB build for Android. An atomic `WriteBatch` and an ordered
`RocksIterator` scan both work correctly on a real emulator (API 36.1, x86_64,
WHPX-accelerated). APK cost is real but bounded and well-understood.

Code lives in `spike/` in this module (`android/docstack-store/spike/`), deliberately
thrown-away-able, not production module structure.

## Binding maturity — resolved

ADR-0003's stated open risk ("RocksDB's Android bindings are less mature than
SQLite's") turned out to have a concrete, current answer rather than being an open
question: **`io.maryk.rocksdb:rocksdb-android`** (GitHub `marykdb/rocksdb-android`,
distinct from the same org's Kotlin/Native `rocksdb-multiplatform` project) is
actively maintained, latest **10.10.1** on Maven Central, tracking upstream RocksDB
releases closely. It's built from RocksDB's real C++ source via CMake/NDK (NDK r29,
confirmed via `file` on the shipped `.so`s), publishes genuine per-ABI native
libraries, and exposes the **same `org.rocksdb` Java API** as the desktop
`rocksdbjni` — so the same code samples/documentation apply, and there's no
alternate API surface to learn.

We did **not** need to do our own NDK/CMake build — task 1's "NDK build integration"
item is satisfied by consuming this prebuilt artifact. A from-scratch native build
would be separate, larger work; not needed here.

## Functional verification (on-device, not just "it links")

Ran a real instrumented test (`RocksDbSpikeTest.kt`) on the `Medium_Phone_API_36.1`
emulator (WHPX-accelerated x86_64, already provisioned locally):

- **Atomic `WriteBatch`**: 5 puts across different keys, one `db.write(...)` commit,
  then read every key back — all present, correct values. (Note: this checks
  *observed* atomicity, not true crash-consistency under a kill -9 mid-write; that's
  a heavier chaos test spec 02's own Verification section already scopes as later,
  non-spike work.)
- **Ordered iteration**: `RocksIterator.seekToFirst()` + `next()` returned keys in
  exact lexical order — the direct analogue of `allDocs`/`changes`, and confirms
  ADR-0001's byte-ordering assumption holds in practice, not just in theory.
- **Timing** (rough signal, not a real benchmark — spec 02 already calls for a
  proper 10k-document benchmark later, that's Phase 1+ work): `openMs=50`,
  `writeBatchMs=1`, `scanMs=2` for a 5-key batch. DB open is the dominant one-time
  cost; the write/scan themselves are sub-few-ms at this scale.

Test report: 1 test, 0 failures, 0 errors.

## APK size delta per ABI

Built release APKs with ABI splits enabled, compared against a byte-identical no-op
baseline app (same manifest, zero dependencies) to isolate RocksDB's actual cost:

| ABI | App APK | Baseline APK | Delta |
| --- | --- | --- | --- |
| `arm64-v8a` | 25.8 MB | 638 KB | **~24.0 MB** |
| `armeabi-v7a` | 19.2 MB | 638 KB | **~17.7 MB** |
| `x86_64` | 27.5 MB | 638 KB | **~25.6 MB** |

Breakdown inside the `arm64-v8a` APK (uncompressed sizes of the packaged `.so`s):

| Library | Size |
| --- | --- |
| `librocksdb.so` | 11.7 MB |
| `librocksdbjni.so` | 10.5 MB |
| `libc++_shared.so` (C++ STL, required since RocksDB is C++) | 1.4 MB |
| `libzstd.so` | 0.73 MB |
| `libsnappy.so` | 0.38 MB |
| `liblz4.so` | 0.15 MB |
| `libbz2.so` | 0.07 MB |

The build initially warned `Unable to strip ... packaging them as they are` for
every one of these — that reads alarming (as if debug symbols were being shipped
unstripped, which would be a real bloat bug), but `file` on the extracted `.so`s
confirms they're **already stripped** by the library publisher (built with NDK r29).
AGP's own strip pass just declined to re-process them (a known AGP/NDK-version-
mismatch quirk), not a sign the numbers above are inflated. **These are the real,
final per-ABI sizes**, not a stripping bug to fix.

Compression codecs (zstd/snappy/lz4/bz2) are statically linked into the build and
account for a combined ~1.3 MB — small relative to `librocksdb`/`librocksdbjni`
themselves. If APK size becomes a real constraint later, a custom build with only
the needed codecs (we'd realistically want at most Snappy or Zstd, not all four)
is the lever, but that reopens the "no from-scratch NDK build needed" simplification
above — worth remembering as a specific, bounded future option rather than assumed
free.

## Version/tooling gotchas hit along the way

- **AGP 9.3.1 requires Gradle 9.5.0 minimum**; the cached Gradle 9.1.0 distribution
  reused from the headless spike failed with an opaque
  `NoClassDefFoundError: org/gradle/features/binding/ProjectTypeBinding` while just
  evaluating the `com.android.application` plugin — not a useful error on its own,
  worth remembering the actual cause if it recurs. Fixed by pointing the wrapper at
  Gradle 9.7.0 (current stable, confirmed via `services.gradle.org/versions/current`).
- **AGP 9.0+ has Kotlin support built in.** Applying `kotlin("android")` alongside
  `com.android.application` now fails outright ("no longer required... not
  compatible with the new DSL"). Just `id("com.android.application")` is correct;
  no separate Kotlin plugin, no `kotlin {}` block needed for basic usage.
- **`io.maryk.rocksdb:rocksdb-android` requires core library desugaring** (
  `isCoreLibraryDesugaringEnabled = true` + `coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.1.5")`)
  — an AAR-metadata-enforced requirement, fails the build clearly if omitted.

## Go/no-go read

**Go.** ADR-0003's open risk is resolved with a real, current, well-tested artifact —
recommend moving its status from "provisional — pending the binding spike" to
confirmed. The ~18-26 MB per-ABI APK cost is real and worth stating plainly in the
ADR (it wasn't quantified before), but it's a known, bounded, one-time cost with an
identified lever (trim compression codecs) if it ever needs to shrink — not a
blocker.

## Reproducing

```bash
cd android/docstack-store/spike
./gradlew :app:connectedAndroidTest   # needs a booted device/emulator
./gradlew :app:assembleRelease :baseline:assembleRelease
# compare app/build/outputs/apk/release/*.apk against baseline/build/outputs/apk/release/*.apk
```
