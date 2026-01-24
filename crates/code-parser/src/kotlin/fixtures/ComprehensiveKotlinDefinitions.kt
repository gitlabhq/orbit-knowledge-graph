package com.project.access;

import java.annotation.AnnotationTarget
import java.time.*
import java.log as logger

object Time {
   val utcClock = Clock.systemUTC()
}

@JvmInline
value class ProjectId(val id: String)

@Target(AnnotationTarget.CLASS)
annotation class Disposable

data class Project(
   private val absolutePath: String,
   private val name: String
) {
   companion object {
       fun default(): Project = Project("~/", "default-project")
   }
}

fun Project.display(): String {
   return "[$absolutePath] $name"
}

const val BASE_URL = "localhost:8000"

val String.urlAndPort: Pair<String, String> 
  get() = split(":").let { it[0] to it[1] }

val httpClient by lazy { HttpClient() }

enum class AccessResult(val message: String) {
   UNKNOWN_PROJECT("Unknown project"),
   ACCESS_EXPIRED("Access expired"),
   ACCESS_OK("Access ok")
}

internal interface IProjectAccessService

@Disposable
class ProjectAccessService(
   private val project: Project
): IProjectAccessService {
   companion object {
       private val logger: Logger = logger<ProjectAccessService>()
   }

   constructor() : this(Project.default())

   private val clock = Time.clock

   fun validateAccess(target: String): AccessResult {
       val requestUrl = "$BASE_URL/access/${project.name}?target=$target&time=${clock.utc()}"
       
       return httpClient
           .get(requestUrl)
           .unsafeInto<AccessResult>()
   }

   fun revokeAccess(target: String) {
       val requestUrl = "$BASE_URL/access/${project.name}/revoke"

       val body = json {
           "target" to target
           "time" to clock.utc()
       }
       
       httpClient
           .post(requestUrl, body, log)
   }
}

val printUrlAndPort = { url, port ->
      println("Service url: $url and port $port")
}

fun main() {
   val project = Project("~/project", "sample")
   println("Loaded ${project.display()}.")

   val (url, port) = BASE_URL.urlAndPort
   printlnUrlAndPort(url, port)

   val service = ProjectAccessService(project)
   println(service.validateAccess(Project.default()))
}
