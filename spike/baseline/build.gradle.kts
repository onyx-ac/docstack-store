plugins {
    id("com.android.application")
}

android {
    namespace = "ac.onyx.docstack.store.spike.baseline"
    compileSdk = 36

    defaultConfig {
        applicationId = "ac.onyx.docstack.store.spike.baseline"
        minSdk = 24
        targetSdk = 36
    }

    splits {
        abi {
            isEnable = true
            reset()
            include("armeabi-v7a", "arm64-v8a", "x86_64")
            isUniversalApk = false
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
    }
}
