package cn.cug.dga.meta.service;

import cn.cug.dga.meta.bean.TableMetaInfo;
import cn.cug.dga.meta.bean.TableMetaInfoForQuery;
import cn.cug.dga.meta.bean.TableMetaInfoPageVo;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 元数据表 服务类
 * </p>
 *
 * @author song
 * @since 2024-02-07
 */
public interface TableMetaInfoService extends IService<TableMetaInfo> {

    void initMetaInfoTables(String schemaName, String assessDate) throws Exception;

    //定义一个方法实现分页查询结果
    List<TableMetaInfoPageVo> queryPageDataForTables(TableMetaInfoForQuery tableMetaInfoForQuery);

    //定义方法实现符合条件查询的分页数据总数的返回
    int queryPageDataForNum(TableMetaInfoForQuery tableMetaInfoForQuery);


    //单表详细信息查询
    TableMetaInfo tableDetailByid(String tableId);

    //定义一个方法实现元数据表的查询，根据库名和考评时间来查 因为考评是每天考评一次 并且可能数据中我们有时候只需要考评某个数据库下的
    //返回值还是tablemetainfo的list因为里面封装了extra的属性

    List<TableMetaInfo> queryMetainfoBydate(String schemaName,String asscessDate);

}
