const {spawn} = require('child_process');
const path = require('path');  // 使用 path 模块来处理路径

const predict = (req, res) => {
    const {features} = req.body;

    if (features && features.length === 10) {
        // 获取 Python 脚本的完整路径
        const pythonScriptPath = '/Users/jinyuanzhang/IdeaProjects/data-platform/data-science/predict.py';

        // 调用 Python 脚本进行预测
        const pythonProcess = spawn(
            '/Users/jinyuanzhang/IdeaProjects/data-platform/data-science/venv/bin/python3',
            [pythonScriptPath, JSON.stringify(features)],
            {cwd: '/Users/jinyuanzhang/IdeaProjects/data-platform/data-science'}
        );

        let result = '';

        // 接收 Python 脚本的输出
        pythonProcess.stdout.on('data', (data) => {
            result += data.toString();
        });

        // 处理 Python 脚本的错误
        pythonProcess.stderr.on('data', (data) => {
            console.error(`stderr: ${data}`);
        });

        // 当 Python 脚本执行完成
        pythonProcess.on('close', (code) => {
            if (code === 0) {
                // 返回预测结果
                res.json({prediction: result.trim()});
            } else {
                res.status(500).json({error: 'Error in prediction process'});
            }
        });
    } else {
        res.status(400).json({error: 'Invalid input data'});
    }
};

module.exports = {predict};