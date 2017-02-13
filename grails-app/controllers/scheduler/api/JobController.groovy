package scheduler.api

import grails.transaction.Transactional
import grails.rest.*
import grails.converters.*
import static groovy.json.JsonOutput.toJson
import java.io.InputStream
import groovy.json.JsonSlurper

@Transactional(readOnly = true)
class JobController {
    def chronosService

    static responseFormats = ['json', 'xml']
    static allowedMethods = [save: "POST", update: "PUT", delete: "DELETE"]
	
    def index() {
        render toJson(chronosService.getJobs(params.ppid, params.pid))
    }
    def save(Job job) {
        def groovy_bin = "/home/spiderdt/work/git/spiderdt-release/opt/groovy/bin/groovy -Dorg.apache.logging.log4j.level=info"
        def job_dir = "/home/spiderdt/work/git/spiderdt-team/data-platform/job-launcher/groovy"
        def (projectId, categoryId) = [params.ppid, params.pid]
        def job_args = job.args ?: []
        switch(categoryId) {
            case 'variable': 
                chronosService.createNowPeriodJob(
                    projectId, categoryId, job.name, "12H", "${groovy_bin} ${job_dir}/variable_job.groovy ${job.name} ${job_args.join(" ")}".toString()
                ) ; break
            case 'ml':
                chronosService.createNowOnetimeJob(
                    projectId, categoryId, job.name, "${groovy_bin} ${job_dir}/ml_job.groovy ${job.name} ${job_args.join(" ")}".toString()
                ) ; break
            case 'dashboard':
                chronosService.createNowOnetimeJob(
                    projectId, categoryId, job.name, "${groovy_bin} ${job_dir}/dashboard_job.groovy ${job.name} ${job_args.join(" ")}".toString()
                ) ; break
        }
        render toJson(job.properties + [projectId: projectId, categoryId:categoryId, id: job.name])
    }
    def show(Job job) {
        if(params.id == "ARGS") { 
            def input_stream = request.inputStream
            def stream_json = new JsonSlurper().parseText(input_stream.text)
            log.info("stream:" + input_stream)
            log.info("stream.args.jobIds" + stream_json.args.jobIds)
            render toJson(chronosService.getMultiJobs(params.ppid,params.pid,stream_json.args.jobIds))
        } else {
            render toJson(chronosService.getJob(params.ppid, params.pid, params.id))
        }
    }
}
