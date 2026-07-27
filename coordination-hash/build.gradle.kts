plugins {
    `java-library`
    `maven-publish`
}

dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
