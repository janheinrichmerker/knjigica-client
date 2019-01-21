plugins {
    kotlin("jvm") version "1.3.11"
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("mbuhot:eskotlin:0.6.0")
    implementation("com.heinrichreimer:elasticsearch-kotlin-dsl:0.1.1")
    implementation("org.elasticsearch.client:elasticsearch-rest-high-level-client:6.5.2")
    implementation("org.apache.logging.log4j:log4j-core:2.11.1")

}
