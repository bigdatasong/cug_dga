package cn.cug.dga.meta.mapper;

import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.bean.TableMetaInfoForQuery;
import cn.cug.dga.meta.bean.TableMetaInfoPageVo;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * <p>
 * 元数据表 Mapper 接口
 * </p>
 *
 * @author song
 * @since 2024-02-07
 */
@Mapper
public interface TableMetaInfoMapper extends BaseMapper<TableMetaInfo> {


    //定义一个方法实现分页查询结果
    List<TableMetaInfoPageVo> queryPageDataForTables(TableMetaInfoForQuery tableMetaInfoForQuery);

    //定义方法实现符合条件查询的分页数据总数的返回
    int queryPageDataForNum(TableMetaInfoForQuery tableMetaInfoForQuery);

    List<TableMetaInfo> queryMetaInfoBydate(@Param("db") String schemaName, @Param("dt") String assessDate);

}
