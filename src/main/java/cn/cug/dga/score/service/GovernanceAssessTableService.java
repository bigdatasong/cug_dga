package cn.cug.dga.score.service;

import cn.cug.dga.score.bean.GovernanceAssessTable;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 表治理考评情况 服务类
 * </p>
 *
 * @author song
 * @since 2024-03-09
 */
public interface GovernanceAssessTableService extends IService<GovernanceAssessTable> {

    //因为前面的步骤中是在mapper中编写了方法 所以在service层中应该编写方法 来调用这个mapper 并且封装号的数据是还缺值得 需要在这里定义一个方法
    //实现所有字段得封装 然后再调用方法 将数据封装到数据库中
    //将数据写入到数据库中是不需要返回值得
    void calScorePerTable(String assessDate);

}
