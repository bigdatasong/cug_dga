package cn.cug.dga.assess.assessor;

import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.utils.AssessParam;
import org.datanucleus.store.rdbms.mapping.java.BooleanMapping;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;

/**
 * author song
 * date 2024/2/28 10:37
 * Desc 定义模板方法 必须有子类 所以设为抽象方法 不能直接new
 */
public abstract class AssessTemplate {

    //在抽象类中定义一个抽象方法 使得子类必须实现该方法 从而实现核心步骤
    protected abstract void assess(AssessParam assessParam,GovernanceAssessDetail assessDetail);

    //以为父类必须做一些公共的事情 所以 也要一个公共方法 ,然后我在外面调用的时候只调用这个公共方法就能实现逻辑,所以我在
    //这个公共方法 调用抽象方法 ,这样的话就可以使得子类调用自己实现的方法

    public GovernanceAssessDetail doassess(AssessParam assessParam){
        GovernanceAssessDetail governanceAssessDetail = new GovernanceAssessDetail();

        // 在父类封装参数
        governanceAssessDetail.setAssessDate(assessParam.getAssessDate());
        governanceAssessDetail.setTableName(assessParam.getTableMetaInfo().getTableName());
        governanceAssessDetail.setSchemaName(assessParam.getTableMetaInfo().getSchemaName());
        governanceAssessDetail.setMetricId(String.valueOf(assessParam.getGovernanceMetric().getId()));
        governanceAssessDetail.setMetricName(assessParam.getGovernanceMetric().getMetricName());
        governanceAssessDetail.setGovernanceType(assessParam.getGovernanceMetric().getGovernanceType());
        governanceAssessDetail.setTecOwner(assessParam.getTableMetaInfo().getTableMetaInfoExtra().getTecOwnerUserName());
        //考评得分可以先赋值 然后根据考评后 子类可以调用修改分数的方法来修改得分 可以先赋值默认得分
        governanceAssessDetail.setAssessScore(BigDecimal.TEN);
        //异常信息 可以通过捕获来赋值 如果说子类方法出现异常就捕获
        try {
            assess(assessParam,governanceAssessDetail);

        }catch (Exception e){
            governanceAssessDetail.setIsAssessException("1");
            //异常打印方法是将异常信息打印到控制台上 我们需要收集异常信息
           // e.printStackTrace();
            // 在其重载方法中 需要一个printwritter 相当于一个支笔
            //stringwriter 相当于一张纸
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            e.printStackTrace(printWriter);
            String exceptionMsg = stringWriter.toString();
            // 还有一个问题 就是由于异常信息有时候会超出长度 而在数据库中对于该字段的最大长度是2000
            //所以这里应该将异常信息的截取0-2000的长度
            governanceAssessDetail.setAssessExceptionMsg(exceptionMsg.substring(0,Math.min(2000,exceptionMsg.length())));
        }
        //对于考评问题项 考评备注 治理处理路径都需要子类来赋值实现
        //但是处理路径可以先赋值好 但是因为链接上有一个id 需要修改 所以和得分一样在子类都需要修改
        governanceAssessDetail.setGovernanceUrl(assessParam.getGovernanceMetric().getGovernanceUrl());

        return  governanceAssessDetail;
    }

    //定义一个子类调用的方法 用于修改分数等逻辑
    //而参数需要的是 修改的分数,考评问题项 考评备注 治理链接还需要一个id 以及是否需要替换治理链接 还有就是
    //前面赋值好的detail
    protected void assessScore(BigDecimal score, String problem, String comment, Boolean isReplace,String id,GovernanceAssessDetail detail){
        //接着复制即可
        detail.setAssessScore(score);
        detail.setAssessProblem(problem);
        detail.setAssessComment(comment);
        if (isReplace) {
            // 需要替换 就将detail的uri关于表id进行替换
            detail.getGovernanceUrl().replace("{id}",id);
        }
    }
}
