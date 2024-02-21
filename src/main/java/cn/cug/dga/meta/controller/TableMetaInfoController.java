package cn.cug.dga.meta.controller;

import cn.cug.dga.meta.bean.TableMetaInfoForQuery;
import cn.cug.dga.meta.bean.TableMetaInfoPageVo;
import cn.cug.dga.meta.service.TableMetaInfoExtraService;
import cn.cug.dga.meta.service.TableMetaInfoService;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.script.ScriptContext;
import java.util.List;

/**
 * <p>
 * 元数据表 前端控制器
 * </p>
 *
 * @author song
 * @since 2024-02-07
 */
@RestController
@RequestMapping("/tableMetaInfo")
public class TableMetaInfoController {

    //手动更新元数据 http://dga.gmall.com/tableMetaInfo/init-tables/g/2024-01-29
    // 注入metainfoservice
    @Autowired
    private TableMetaInfoService tableMetaInfoService;

    @Autowired
    private TableMetaInfoExtraService tableMetaInfoExtraService;
    @PostMapping("/init-tables/{schamaName}/{assessDate}")
    public String initMetaInfoTables(@PathVariable("schamaName") String schemaName,@PathVariable("assessDate") String assessDate) throws Exception {

        tableMetaInfoService.initMetaInfoTables(schemaName,assessDate); //该方法是不需要返回值的，因为我只是更新元数据后是要保存到数据库中

        //同步辅助信息
        tableMetaInfoExtraService.initExtraMetaInfo(schemaName);

        return "success";
    }

    //http://dga.gmall.com/tableMetaInfo/table-list?schemaName=g&tableName=g&dwLevel=ODS&pageSize=20&pageNo=1

    //定义一个接口方法 实现对表信息查询列表 其中包括分页信息的查询
    //因为传递的参数比较多可以用一个bean来封装
    @GetMapping("/table-list")
    public Object tableInfoList(TableMetaInfoForQuery tableMetaInfoForQuery){

        // 对于返回格式的封装，如果说返回的格式是一个{} 那么 就用map或者jsonobject封装，jsonobject本质就是map
        //如果说返回的是一个[] 就用list或者 jsonarray封装

        // 该需求中返回的是一个{} 内容有符合条件的总数以及分页查询的结果,所以说需要两个查询结果，然后再封装到jsonobject中，
        //也即需要service层中两个方法分别获取总数以及分页查询的结果

        List<TableMetaInfoPageVo> tableMetaInfoPageVos = tableMetaInfoService.queryPageDataForTables(tableMetaInfoForQuery);

        int num = tableMetaInfoService.queryPageDataForNum(tableMetaInfoForQuery);
        //获取到以后封装指定格式的数据

        JSONObject jsonObject = new JSONObject();

        jsonObject.put("total",num);
        jsonObject.put("list",tableMetaInfoPageVos);


        return jsonObject;
    }



}
