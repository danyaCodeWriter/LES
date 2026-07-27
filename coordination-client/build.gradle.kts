plugins {
    `java-library`
    `maven-publish`
    id("com.google.protobuf") version "0.9.4"
}

val grpcVersion = "1.67.1"
val protobufVersion = "3.25.5"

dependencies {
    api(project(":coordination-hash"))

    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.2")
    implementation("org.slf4j:slf4j-api:2.0.16")

    implementation(platform("io.grpc:grpc-bom:$grpcVersion"))
    implementation("io.grpc:grpc-netty-shaded")
    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-stub")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
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

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
