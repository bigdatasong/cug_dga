package cn.cug.dga.assess.assessor.security;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.utils.AssessParam;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.util.TrackingUriPlugin;
import org.datanucleus.enhancer.methods.IsXXX;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * author song
 * date 2024/3/1 21:51
 * Desc 判断文件或者目录的权限阈值是否超过了指定阈值
 *      {"dir_permission":"755","file_permission":"644"}
 */
@Component("FILE_ACCESS_PERMISSION")
public class CheckFilePermission extends AssessTemplate {
    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) throws Exception {
        // 首先获取文件和目录的权限阈值
        JSONObject jsonObject = JSON.parseObject(assessParam.getGovernanceMetric().getMetricParamsJson());

        Integer dirPermission = jsonObject.getInteger("dir_permission");
        Integer filePermission = jsonObject.getInteger("file_permission");
        //然后要想获取到权限 必须使用hdfs的客户段来获取
        //获取当前表的路径 以及当前表的所属者 用于获取hdfs的客户端
        String tableFsPath = assessParam.getTableMetaInfo().getTableFsPath();
        String tableFsOwner = assessParam.getTableMetaInfo().getTableFsOwner();
        FileSystem hdfs = FileSystem.get(new URI(tableFsPath), new Configuration(), tableFsOwner);

        //在hdfs中 需要得到某一路径下的文件或者状态 其中filestatus 就表示的是hdfs中的文件状态 通过这个状态可以判断出该层是否是目录
        // 并且也可以获取到该层的权限
        //因为我们需要获取到表目录以及表目录下所有文件的权限信息 再返回最大的权限信息 所以肯定涉及到递归
        //所以肯定需要一个递归的方法 来检查当前路径下的权限
        //先获取到表目录所在的filestatus  因为我们是需要判断目录和文件的权限是否超过 所以说表这一层的路径他也是有权限的 所以也是需要判断
        //并且不管是目录权限也好 文件权限也好 其判断权限是否超过的逻辑是一样的 所以说
        //先定义递归的起点就是获取当前表路径下的filestatus
        FileStatus tableFileStatus = hdfs.getFileStatus(new Path(tableFsPath));
        //然后定义递归方法 将当前filestauts状态传入 并且因为要获取子目录下的filestatus 所以文件客户端也需要传入，然后因为这个方法是要最终判断
        //权限是否超过的 所以权限阈值也需要传入

        //调用递归方法以后 其实不需要返回结果 直接把list传进去 里面是可以放入值的 所以不需要返回结果即可
        List<JSONObject> resultPermission = new ArrayList<>();

        //调用递归方法
        CheckPermission(resultPermission, tableFileStatus, hdfs, dirPermission, filePermission);

        //如果说这个result不为空 就说明超出了阈值 就需要判0分了
        if (!resultPermission.isEmpty()){
            assessScore(BigDecimal.ZERO,"目录中有文件或者目录超出了权限阈值",resultPermission.toString(),true,null,assessDetail);
        }

    }

    //定义递归方法
    private void CheckPermission(List<JSONObject> resultPermission,FileStatus fileStatus,FileSystem hdfs,Integer dirPermission,Integer filePermission) throws IOException {

        // 肯定先要判断是否是目录或者文件 因为二者的权限阈值不一样
        if (fileStatus.isDirectory()){
            //定义一个方法 实现权限阈值的判断
            JSONObject jsonObject = comparePermission(fileStatus, dirPermission);

            //在这里不管是文件也好还是目录也好 都返回其超出阈值的信息 可以用list来接收 一个list里面存放了 jsonobjec 一个list表示了这个表的权限超出信息
            //调用者根据这个list信息来判分
            if (jsonObject != null){

                resultPermission.add(jsonObject);
            }

            //然后应该递归调用该方法 因为目录下还有文件
            //先获取当前目录下的所有文件信息
            FileStatus[] fileStatuses = hdfs.listStatus(fileStatus.getPath());
            for (FileStatus status : fileStatuses) {
                //然后循环遍历 再去调用递归方法 判断权限阈值是否超过
                CheckPermission(resultPermission,status,hdfs,dirPermission,filePermission);

            }

        }else if (fileStatus.isFile()){
            //如果是文件 那同样也是调用判断权限是否超过阈值的方法
            JSONObject jsonObject = comparePermission(fileStatus, filePermission);

            //在这里不管是文件也好还是目录也好 都返回其超出阈值的信息 可以用list来接收 一个list里面存放了 jsonobjec 一个list表示了这个表的权限超出信息
            //调用者根据这个list信息来判分
            if (jsonObject != null){
                resultPermission.add(jsonObject);
            }
        }
    }

    // 可以发现不管是目录权限也好还是文件权限也好 其判断逻辑是一样的 所以说 可以抽出一个方法来实现判断阈值
    // 并且方便起见 可以定义如果说超出了权限时的各种信息的返回值 用来返回记录权限阈值信息 返回的具体信息可以是超出权限时
    //每一步到达的文件路径 文件权限 以及权限超的信息 所以返回的这么多值可以返回一个jsonobject
    //因为要获取权限 所以filestatus需要 另外阈值也需要
    private JSONObject comparePermission(FileStatus fileStatus,Integer limitPermission){
        JSONObject jsonObject = new JSONObject();
        //获取权限阈值
        FsPermission permission = fileStatus.getPermission(); //这个阈值不是我们想要的数字阈值
        //将字符阈值转为数字阈值
        // 这是第一位 int ordinal = permission.getUserAction().ordinal();
        // 这是第二位 int ordinal = permission.getGroupAction().ordinal();
        // 这是第三位 int ordinal = permission.getOtherAction().ordinal();
        //需要将这三位拼接在一块 以为拼接可以用字符串的拼接方式 +
        //为了防止用+ 号 表示来运算 将其转为string 然后转回integer
        Integer integerPermission = Integer.valueOf("" + permission.getUserAction().ordinal() + permission.getGroupAction().ordinal() + permission.getOtherAction().ordinal());

        if (integerPermission > limitPermission){
            //超出权限 记录超出权限的信息
            jsonObject.put("filePath",fileStatus.getPath());
            jsonObject.put("当前权限",integerPermission);
            jsonObject.put("msg","当前文件超出了其权限阈值"+ limitPermission);

            return jsonObject;
        }

        //如果说没有超出 就可以返回nul
        return null;

    }
}
