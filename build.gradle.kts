plugins {
    id("com.android.library") version "9.3.1"
    id("org.jetbrains.kotlin.plugin.serialization") version "2.2.10"
}

android {
    namespace = "ac.onyx.docstack.store"
    // io.maryk.rocksdb:rocksdb-android's AAR metadata requires compileSdk 36+
    // (confirmed by the spike, which used 36 for the same reason) - overrides
    // android/CLAUDE.md's stated compileSdk 35, forced by this dependency, not a
    // style choice.
    compileSdk = 36

    defaultConfig {
        minSdk = 24
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
        isCoreLibraryDesugaringEnabled = true
    }

    // DocumentStoreConformanceTest lives in src/sharedTest: the in-memory store only
    // needs the plain JVM (test), the RocksDB-backed one only runs on-device
    // (androidTest, native .so) - one abstract class, no duplication either way.
    sourceSets {
        getByName("test") {
            kotlin.srcDir("src/sharedTest/kotlin")
        }
        getByName("androidTest") {
            kotlin.srcDir("src/sharedTest/kotlin")
        }
    }
}

kotlin {
    explicitApi()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
    implementation("io.maryk.rocksdb:rocksdb-android:10.10.1")
    coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.1.5")

    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
    testImplementation("junit:junit:4.13.2")

    androidTestImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
    androidTestImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test:runner:1.7.0")
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
}
