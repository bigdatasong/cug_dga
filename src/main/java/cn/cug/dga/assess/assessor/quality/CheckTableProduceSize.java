package cn.cug.dga.assess.assessor.quality;

import cn.cug.dga.assess.assessor.AssessTemplate;
import cn.cug.dga.assess.bean.GovernanceAssessDetail;
import cn.cug.dga.constant.MetaConstant;
import cn.cug.dga.utils.AssessParam;
import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * author song
 * date 2024/3/8 21:20
 * Desc
 * 必须日分区表
 *
 * 前一天产生的数据量，超过前x天平均产出量{upper_limit}% ，或低于{lower_limit}%  ，则给0分，其余10分
 */
@Component("PRODUCE_DATA_SIZE")
public class CheckTableProduceSize extends AssessTemplate {

    @Override
    protected void assess(AssessParam assessParam, GovernanceAssessDetail assessDetail) throws Exception {
        // 首先必须是日分区表 其次就是要获取到前一天的表的产出的数据量 就是tablesize大小
        //然后获取参数
        // 然后就是在hdfs中 我们发现在一个表目录下 并且是日分区表的目录下 是有多个分区目录的，所以说该指标获取前几天的数据量的时候应该是
        //把前几天的分区目录得到其表的size

        // 获取存储周期 判断是否是日分区表
        String lifecycleType = assessParam.getTableMetaInfo().getTableMetaInfoExtra().getLifecycleType();

        if (!lifecycleType.equals(MetaConstant.LIFECYCLE_TYPE_DAY)){
            return;
        }
        //获取参数 {"days":7,"{upper_limit}":70,"{lower_limit}":50}
        String metricParamsJson = assessParam.getGovernanceMetric().getMetricParamsJson();
        Integer day = JSON.parseObject(metricParamsJson).getInteger("day");
        BigDecimal upperLimit = JSON.parseObject(metricParamsJson).getBigDecimal("upper_limit");
        BigDecimal lowerLimit = JSON.parseObject(metricParamsJson).getBigDecimal("lower_limit");

        //需要一个hdfs客户端
        String tableFsPath = assessParam.getTableMetaInfo().getTableFsPath();
        String tableFsOwner = assessParam.getTableMetaInfo().getTableFsOwner();
        FileSystem hdfs = FileSystem.get(new URI(tableFsPath), new Configuration(), tableFsOwner);

        //定义一个方法 实现 返回表中每一个分区下的大小 可以使用一个list返回 list存放的是每一个分区 以及分区对应的大小
        //所以自定义类 类中的属性为dt 以及 dt下的大小
        //该方法首先days需要传入 用于标识需要获取哪些天的数据 另外考评时间需要传入 因为前几天的定义肯定是以考评日期往前推的
        //还有就是需要获取文件大小 就需要将hdfs的客户端传入
        //获取考评日期
        String assessDate = assessParam.getAssessDate();
        List<partitionSize> partitionSizes =  getPartitionDataSize(hdfs,day,assessDate,tableFsPath);
        //获取到了分区以及对应的分区数据大小

        //获取最近一天的分区数据量 因为list集合是有序的 我第一个加的就是最近一天的数据
        partitionSize LastDaypartitionSize = partitionSizes.get(0);
        Long dtSizeForLast = LastDaypartitionSize.getDtSize();

        //求离最近一天的前x天的数据的平均值
        //list集合的操作使用streamapi 可以实现去过滤以及平均值的求出
        //具体的思路就是说先过滤出前x天的数据
        //如果说只有一天的数据需要不应该进行以下代码
        if (partitionSizes.size() <= 1){
            //只调度了一天
            return ;
        }

        String recentDayDt = LocalDate.parse(assessDate).minusDays(1).toString();
        //或者说我在下面的过滤的代码中 在使用完平均值方法后  再调用orelse(0) 方法就不会抛异常
        double avgToSize = partitionSizes.stream().filter(p -> !p.getDt().equals(recentDayDt))
                .mapToLong(p -> p.getDtSize())
                .average()
                .getAsDouble();


        //获取平均产出量的上下阈值
        BigDecimal upperAvg = BigDecimal.valueOf(avgToSize).multiply(upperLimit.add(BigDecimal.valueOf(100))).movePointLeft(2);

        BigDecimal lowerAvg = BigDecimal.valueOf(avgToSize).multiply(lowerLimit.add(BigDecimal.valueOf(100))).movePointLeft(2);

        if (BigDecimal.valueOf(dtSizeForLast).compareTo(upperAvg) == 1 ||
                BigDecimal.valueOf(dtSizeForLast).compareTo(lowerAvg) == -1
           ){
            //说明超出了上限 或者低于下限
            /**
             *  String msg = "dt=%s数据的产生量:%s,超过了过去%d的平均产出量%s,高于阈值%s%%,或者低于阈值%s%%";
             *             String str = String.format(msg, recentPartitionDt, recentPartitionSize.size,days,avgSize ,upperLimit, lowerLimit);
             *             assessScore(BigDecimal.ZERO,"最近1天数据产出量超过阈值",str,detail,false,null);
             */

            //定义一个字符串 实现commpent的字段
            //因为在字符串中 %是特殊字符 所以用%% 标识的是一个%
            String msg = "dt = %s的数据的产生量：%s,超过了过去%d的平均产出量%s,高于阈值%s%%，或者低于阈值%s%%";
            String str = String.format(msg, recentDayDt, dtSizeForLast, day, avgToSize, upperAvg, lowerAvg);
            assessScore(BigDecimal.ZERO,"最近1天数据产出量超过阈值",str,false,null,assessDetail);

        }


    }



    // 获取表中各个分区的大小
    private List<partitionSize> getPartitionDataSize(FileSystem hdfs, Integer day, String assessDate,String tableFsPath) throws IOException {

        List<partitionSize> list = new ArrayList<>();

        //思路就是根据考评日期往前推days天 这些天都需要获取到其分区大小
        //因为我们最终是要封装partitionSize需要的就是每个分区dt以及每个分区大小
        for (Integer i = 0; i <= day; i++) {
            //因为要求的是考评日期前一天 至 考评日期前一天的前days天所以上述要小于等于
            //获取每一天的dt时间
            String dt = LocalDate.parse(assessDate).minusDays(1).minusDays(i).toString();
            // 因为获取分区里面的数据大小的逻辑是一样的 所以定义一个方法实现
            //获取每个分区大小的方法 该方法可能会涉及到递归 所以我们只需要先将分区路径下的文件传入即可  如果说涉及到文件的遍历需要将hdfs传入
            //获取分区完整路径 需要将表路径也需要传入上述方法中
            Path dtpath = new Path(tableFsPath, "dt=" + dt);
            //这是一个递归遍历方法 所以需要hdfs 另外直接列出这个路径的下文件 将文件数组传入进行递归
            //这里的dtpath有可能为空
            if (hdfs.exists(dtpath)){

                FileStatus[] fileStatuses = hdfs.listStatus(dtpath);
                Long size = statSize(fileStatuses,hdfs);

                //获取到每个分区的大小以及分区的日期以后开始封装partitionsize
                list.add(new partitionSize(dt,size));
            }
        }

        return list;
    }

    //创建递归方法 对某路径下的文件进行递归获取文件大小
    private Long statSize(FileStatus[] fileStatuses, FileSystem hdfs) throws IOException {
        //定义一个全局的long大小
        Long size = 0L;

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()){
                size += fileStatus.getLen();
            }else if (fileStatus.isDirectory()){
                //继续递归 递归前将当前路径下的文件数组传入
                FileStatus[] subFileStatus = hdfs.listStatus(fileStatus.getPath());
                statSize(subFileStatus,hdfs);
            }
        }

        return size;
    }

    //定义一个静态的内部类 封装dt 以及dt下的size
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class partitionSize{
        private String dt;
        private Long dtSize;
    }
}



