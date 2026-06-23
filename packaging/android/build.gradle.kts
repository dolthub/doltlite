plugins {
    id("com.android.library") version "8.5.2"
    id("org.jetbrains.kotlin.android") version "2.0.20"
    id("maven-publish")
    id("signing")
}

// Version is supplied by the release workflow: -PdoltliteVersion=x.y.z.
val doltliteVersion = (findProperty("doltliteVersion") as String?) ?: "0.0.0"

android {
    namespace = "com.dolthub.doltlite"
    compileSdk = 34

    defaultConfig {
        minSdk = 21
    }

    // libdoltlite.so per ABI is produced by build-libs.sh into src/main/jniLibs.
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }

    publishing {
        singleVariant("release") {
            withSourcesJar()
        }
    }
}

dependencies {
    // JNA provides the FFI bindings to libdoltlite; the @aar artifact bundles
    // JNA's own native dispatcher for Android.
    api("net.java.dev.jna:jna:5.14.0@aar")
}

publishing {
    publications {
        create<MavenPublication>("release") {
            groupId = "com.dolthub"
            artifactId = "doltlite-android"
            version = doltliteVersion

            afterEvaluate {
                from(components["release"])
            }

            pom {
                name.set("DoltLite for Android")
                description.set("SQLite with Dolt version control (branches, commits, merge, diff) for Android.")
                url.set("https://github.com/dolthub/doltlite")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                scm {
                    url.set("https://github.com/dolthub/doltlite")
                }
            }
        }
    }
    repositories {
        maven {
            name = "sonatype"
            url = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
            credentials {
                username = System.getenv("SONATYPE_USERNAME")
                password = System.getenv("SONATYPE_PASSWORD")
            }
        }
    }
}

signing {
    val key = System.getenv("SIGNING_KEY")
    val password = System.getenv("SIGNING_PASSWORD")
    if (!key.isNullOrEmpty()) {
        useInMemoryPgpKeys(key, password)
        sign(publishing.publications["release"])
    }
}
