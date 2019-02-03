buildscript {
    repositories {
        mavenCentral()
    }
}

group = "de.unihalle.informatik.bigdata.knjigica"
version = "0.1.1-alpha01"

allprojects {
    repositories {
        jcenter()
        mavenCentral()
        maven("http://dl.bintray.com/mbuhot/maven")
        maven("https://jitpack.io")
    }

    afterEvaluate {
//        tasks.withType<KotlinCompile> {
//            kotlinOptions.jvmTarget = "1.8"
//        }
    }

}
