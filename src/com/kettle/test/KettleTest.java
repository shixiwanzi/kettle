package com.kettle.test;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

/**
 * Kettle入门示例
 * @author heshixi
 *
 */
public class KettleTest {

    public static void main(String[] args) {
        String file = "E:\\testDB.ktr";
        KettleTest.runTransformation(file, args);
    }
 
    /** 
     * Run an exist transformation. 
     *  
     * @param fileURL 
     */
    public static void runTransformation(String fileURL, String[] params) {
        try {
            //初始化kettle环境
            KettleEnvironment.init();
            EnvUtil.environmentInit();
            //创建ktr元对象
            TransMeta transMeta = new TransMeta(fileURL);
            //创建ktr
            Trans trans = new Trans(transMeta);
            //执行ktr
            trans.execute(params);
            //等待执行完毕
            trans.waitUntilFinished();
            if (trans.getErrors() > 0) {
                System.out.println("runTransformation（）方法执行文件" + fileURL + "时，出现运行时错误：");
                throw new RuntimeException("There were errors during transformation execution.");
            }
        } catch (KettleException ex) {
            ex.printStackTrace();
        } finally {
            System.out.println("使用executeTrans(）方法执行" + fileURL + ")结束");  
        }
    }
    
    /** 
     * Run an exits job. 
     *  
     * @param fileURL 
     */  
    public static void runJobs(String fileURL) {  
        System.out.println("------>使用executeJobs(）方法执行文件：" + fileURL + " 开始");  
        try {  
            // 初始化kettle环境  
            KettleEnvironment.init();
            EnvUtil.environmentInit();  
            // 创建kjb元对象  
            JobMeta jobMeta = new JobMeta(fileURL, null);  
            // 创建kjb  
            Job job = new Job(null, jobMeta);  
            job.start();  
            //等待执行完毕  
            job.waitUntilFinished();  
            if (job.getErrors() > 0) {  
                System.out.println("runTransformation（）方法执行文件" + fileURL + "时，出现运行时错误：");  
                throw new RuntimeException("There were errors during transformation execution.");  
            }  
        } catch (KettleException e) {  
            System.out.println("使用executeJobs（）方法执行文件<" + fileURL + ">出错！");  
            e.printStackTrace();  
        } finally {  
            System.out.println("----->使用executeJobs(）执行文件：" + fileURL + " 结束 ");  
        }  
    }    

}
