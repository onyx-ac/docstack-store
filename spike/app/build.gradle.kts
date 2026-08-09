plugins {
    id("com.android.application")
}

android {
    namespace = "ac.onyx.docstack.store.spike"
    compileSdk = 36

    defaultConfig {
        applicationId = "ac.onyx.docstack.store.spike"
        minSdk = 24
        targetSdk = 36
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    // APK-delta-per-ABI measurement (spec 02 task 1): split so each ABI's APK only
    // carries its own .so, then compare sizes.
    splits {
        abi {
            isEnable = true
            reset()
            include("armeabi-v7a", "arm64-v8a", "x86_64")
            isUniversalApk = true
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
        isCoreLibraryDesugaringEnabled = true
    }
}

dependencies {
    implementation("io.maryk.rocksdb:rocksdb-android:10.10.1")
    coreLibraryDesugaring("com.android.tools:desugar_jdk_libs:2.1.5")
    androidTestImplementation("androidx.test:runner:1.7.0")
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
}
