package scheduler.api

import grails.transaction.Transactional
import groovyx.net.http.AsyncHTTPBuilder
import groovyx.net.http.HTTPBuilder
import static groovyx.net.http.Method.GET
import static groovyx.net.http.Method.PUT
import static groovyx.net.http.Method.VALUE
import static groovyx.net.http.Method.POST
import static groovyx.net.http.Method.DELETE
import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.ContentType.VALUE
import static groovyx.net.http.ContentType.TEXT
import static groovy.json.JsonOutput.toJson
import groovy.json.JsonSlurper


@Transactional
class ChronosService {
    
    def http
  //  ChronosService() {
    //    http = new HTTPBuilder('http://192.168.1.2:8080')
     //   http.ignoreSSLIssues()
   // }
  
    ChronosService() {
        http = new AsyncHTTPBuilder(poolSize:4, uri:'http://192.168.1.2:8080')
        http.ignoreSSLIssues()
    }

    def graph_data(){
        def chronos_text_future = http.request(GET,TEXT){req -> uri.path = '/scheduler/graph/csv' }
        while (! chronos_text_future.done) {
           log.info("waiting 0.1 seconds..., try again")
           Thread.sleep(100)
       }
       def chronos_text = chronos_text_future.get().text
       log.debug("recive chronos return: " + chronos_text) 
       chronos_text.split("\n").findAll{it =~ /^node,\w+-\w+-/}.collect{it.split(",").drop(1)}
    }
    def graph_node(){ graph_data().collect{it[0]} } 

    def graph_jobs(){
        def chronos_json_future =  http.request(GET,JSON){req -> uri.path = '/scheduler/jobs' }
        while (! chronos_json_future.done) {
           log.info("waiting 0.1 seconds..., try again")
           Thread.sleep(100)
       }
        def chronos_json = chronos_json_future.get()
        log.debug("receive chronos return: " + chronos_json.toString())
        chronos_json
    }
    
    def getProjects() {
        graph_node().collect{it.split("-")[0]}.unique()
    }

    def getCategories(projectId) {
        graph_node().findAll{it.startsWith(projectId + "-")}.collect{it.split("-")[1]}.unique()
    }

    def getJobs(projectId, categoryId) {
         graph_node().findAll{it.startsWith(projectId + "-" + categoryId + "-")}.collect{it.split("-", 3)[2]}.unique()
    }


    def getMultiJobs(projectId, categoryId, jobIds){
        def graph_jobs = graph_jobs()
        def graph_data =  graph_data()
        jobIds.collect {jobId ->
            def job_info = graph_jobs.find{it.name == "${projectId}-${categoryId}-${jobId}"}.with {
                               [name: it.name.split("-").last(),
                                lastSuccess_ts: it.lastSuccess,
                                lastError_ts: it.lastError,
                                successCount: it.successCount,
                                errorCount: it.errorCount,
                                args: it.command.split(" ").drop(1)]}
            def createTime = graph_jobs.find{it.name == "${projectId}-${categoryId}-${jobId}"}.with{
                                  [createTime_ts: new JsonSlurper().parseText(it.description ?: "{}").createTime_ts]
                                }
            def status =  graph_data.find{it[0].startsWith(projectId + "-" + categoryId + "-" + jobId)}.drop(1)
            def job_status = [status:status[1]!= "idle" ? status[1] : status[0]]
                //[*:name, status:job_status, *:lastSuccess_one, *:lastError_one, *:successCount, *:errorCount, *:createTime, *:args]
                [*:job_info,*:createTime,*:job_status]
        }
    }

    def getJob(projectId, categoryId, jobId){
        getMultiJobs(projectId, categoryId, [jobId])[0]
    }

    def createJob(projectId, categoryId, name, args) {
        def job_json_future =  http.request(POST,JSON){req ->
            uri.path = '/scheduler/iso8601'
            TimeZone.setDefault(TimeZone.getTimeZone('UTC'))
            def create_time = use (groovy.time.TimeCategory) { (new Date()).format("yyyy-MM-dd'T'HH:mm:ss") }
            body = [name: "${projectId}-${categoryId}-${name}".toString()] + [description: '"' + toJson([createTime_ts: create_time]) + '"'] + args
        }
         while (! job_json_future.done) {
           log.info("waiting 0.1 seconds..., try again")
           Thread.sleep(100)
       }
       def job_json = job_json_future.get()
       job_json
    }

    def createNowOnetimeJob(projectId, categoryId, name, command) {
        createJob(projectId, categoryId, name, [command: command, schedule: "R/9999-01-01T00:00:00Z/P"])
        def nowJob_json_future = http.request(PUT,JSON){req ->
            uri.path = "/scheduler/job/${projectId}-${categoryId}-${name}".toString()
        }
         while (! nowJob_json_future.done) {
           log.info("waiting 0.1 seconds..., try again")
           Thread.sleep(100)
       }
        def nowJob_json = nowJob_json_future.get()
        nowJob_json
    }

    def createNowPeriodJob(projectId, categoryId, name, hms, command) {
        TimeZone.setDefault(TimeZone.getTimeZone('UTC'))
        def after_1m = use (groovy.time.TimeCategory) { (new Date() + 1.minute).format("yyyy-MM-dd'T'HH:mm:ss'Z'") }
        createJob(projectId, categoryId, name, [command: command, schedule: "R/${after_1m}/PT${hms}".toString()])
    }

    def createOnetimeJob(projectId, categoryId, name, command, schedule) {
    }

    def createPeriodJob(projectId, categoryId, name, command, schedule) {
    }
}
