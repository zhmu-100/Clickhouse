plugins {
    kotlin("jvm") version "1.8.0"
    kotlin("plugin.serialization") version "1.8.10"
    id("io.ktor.plugin") version "2.2.4"
    application
    id("com.ncorti.ktfmt.gradle") version "0.11.0"
    jacoco
    id("org.jetbrains.dokka") version "1.9.20"
}

group = "org.clickhouse"
version = "1.0.0"

repositories {
    maven { url = uri("https://jitpack.io") }
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))

    implementation("io.ktor:ktor-server-core-jvm:2.2.4")
    implementation("io.ktor:ktor-server-netty-jvm:2.2.4")
    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("io.github.cdimascio:dotenv-kotlin:6.4.1")
    implementation("io.ktor:ktor-server-content-negotiation-jvm:2.2.4")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:2.2.4")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.5.0")
    implementation("ru.yandex.clickhouse:clickhouse-jdbc:0.3.2")
	  implementation("com.github.poplopok:Logger:1.0.6")

    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
    testImplementation("io.ktor:ktor-server-tests-jvm:2.2.4")
    testImplementation("io.mockk:mockk:1.13.5")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
    testImplementation(kotlin("test"))

    testImplementation("io.ktor:ktor-server-test-host:2.2.4")
    testImplementation("io.ktor:ktor-client-core:2.2.4")
    testImplementation("io.ktor:ktor-client-cio:2.2.4")
    testImplementation("io.ktor:ktor-client-content-negotiation:2.2.4")
    testImplementation("io.ktor:ktor-client-json:2.2.4")
    testImplementation("io.ktor:ktor-client-serialization:2.2.4")
    testImplementation("io.ktor:ktor-serialization-kotlinx-json:2.2.4")
}




tasks.test {
    useJUnitPlatform()
}

configure<JacocoPluginExtension> {
    toolVersion = "0.8.8"
}

afterEvaluate {
    fun configureJacocoClassDirectories(taskName: String) {
        tasks.named<JacocoReport>(taskName) {
            classDirectories.setFrom(
                files(classDirectories.files.map {
                    fileTree(it) {
                        exclude(
                            "org/clickhouse/connection/ClickhouseConfig.class",
                            "org/clickhouse/connection/ClickhouseConnection.class",
                            "org/clickhouse/service/ClickhouseService.class",
                            "org/clickhouse/service/IClickhouseService.class",
                            "org/clickhouse/api/ApiServerKt.class"
                        )
                    }
                })
            )
        }
    }

    configureJacocoClassDirectories("jacocoTestReport")
    tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
        classDirectories.setFrom(
            files(classDirectories.files.map {
                fileTree(it) {
                    exclude(
                        "org/clickhouse/connection/ClickhouseConfig.class",
                        "org/clickhouse/connection/ClickhouseConnection.class",
                        "org/clickhouse/service/ClickhouseService.class",
                        "org/clickhouse/service/IClickhouseService.class",
                        "org/clickhouse/api/ApiServerKt.class"
                    )
                }
            })
        )
        }
    }


tasks.check {
    dependsOn(tasks.named("jacocoTestCoverageVerification"))
}

application {
    mainClass.set("org.clickhouse.api.ApiServerKt")
}

kotlin {
    jvmToolchain(17)
}
