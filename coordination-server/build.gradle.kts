plugins {
    java
    id("org.springframework.boot") version "3.3.4"
    id("io.spring.dependency-management") version "1.1.6"
    id("com.google.protobuf") version "0.9.4"
}

val grpcVersion = "1.67.1"
val protobufVersion = "3.25.5"
val jetcdVersion = "0.8.5"
val testcontainersVersion = "1.20.4"

dependencyManagement {
    imports {
        mavenBom("io.grpc:grpc-bom:$grpcVersion")
        mavenBom("org.testcontainers:testcontainers-bom:$testcontainersVersion")
    }
}

dependencies {
    implementation(project(":coordination-hash"))

    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    implementation("org.springdoc:springdoc-openapi-starter-webflux-ui:2.6.0")
    implementation("io.etcd:jetcd-core:$jetcdVersion")

    implementation("net.devh:grpc-server-spring-boot-starter:3.1.0.RELEASE")
    implementation("io.grpc:grpc-netty-shaded")
    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-stub")
    implementation("javax.annotation:javax.annotation-api:1.3.2")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers")
}

sourceSets {
    main {
        proto {
            srcDir("${rootProject.projectDir}/proto")
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

tasks.named<ProcessResources>("processResources") {
    from("${rootProject.projectDir}/openapi") {
        include("openapi.yaml")
        into("static")
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-parameters")
}

tasks.named<Jar>("jar") {
    enabled = false
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveBaseName.set("coordination-server")
}
