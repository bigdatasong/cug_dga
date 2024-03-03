package cn.cug.dga.utils;


import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.util.ArrayList;
import java.util.Collections;

/**
 * author song
 * date 2024/3/3 15:04
 * Desc 定义sql解析器的工具类
 */
public class SqlParser {

    //定义一个方法 应该是静态方法 方便外面进行调用
    public static void parseSql(String sql, Dispatcher dispatcher) throws Exception {
        // sql解析有特定的遍历步骤 需要引入依赖hive exec的sql解析的依赖
        // 用遍历器遍历整个语法树
        //下面就是一个图遍历者 需要参数dispather 需要实现dispather dispather中有方法dispatch 表示遍历的每个节点需要处理的逻辑
        //所以参数中需要传递这个dispatch
        //另外还需要一个节点表示从这个节点来说遍历 这个节点的类型就是ASTNode类型
        //节点的获取可以通过参数传递一个sql再通过sql解析器得到节点即可
        //所以该方法的参数有sql 以及dispatch

        ParseDriver parseDriver = new ParseDriver();

        //这样就获取到了语法树的根节点 由语法树的知识我们可以知道 语法树根节点是nil 下面会有两个节点分别是
        //tokqueryNode 以及 eof  eof表示的结束的位置 不需要对其进行图遍历 
        ASTNode node = parseDriver.parse(sql);
        //获取toquerynode 在其节点下进行遍历  getchild 方法中 参数0 表示的左节点 1 表示右节点
        //方法返回值是tree类型 但是其方法比较少 可以强转为astnode类型 其中方法更丰富

        ASTNode tokQueryNode = (ASTNode) node.getChild(0);
        //将 tokquerynode节点交给图遍历者以及将dispather 传入进行遍历
        GraphWalker ogw = new DefaultGraphWalker(dispatcher);
        ogw.startWalking(Collections.singletonList(tokQueryNode), null);

    }




}
