package com.hmdp.utils;


import com.aliyun.credentials.provider.AlibabaCloudCredentialsProvider;
import com.aliyun.dysmsapi20170525.Client;
import com.aliyun.dysmsapi20170525.models.SendSmsRequest;
import com.aliyun.dysmsapi20170525.models.SendSmsResponse;
import com.aliyun.tea.TeaException;
import com.aliyun.teaopenapi.models.Config;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 使用阿里云发送短信
 *
 * @author aoao
 * @create 2025-06-03-17:14
 */
@Component
@Slf4j
@Data
@ConfigurationProperties(prefix = "ali.sms")
public class AliSmsUtils {

    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String signName;
    private String templateCode;

    // 显式传递 AccessKey 初始化 Client
    public Client createClient() throws Exception {
        com.aliyun.teaopenapi.models.Config config = new com.aliyun.teaopenapi.models.Config()
                .setAccessKeyId(accessKeyId)
                .setAccessKeySecret(accessKeySecret)
                .setEndpoint(endpoint);

        return new com.aliyun.dysmsapi20170525.Client(config); // 确保导入正确的 Client 类
    }

    // 发送短信方法
    public void sendVerificationCode(String phoneNumber, String code) {
        try {
            Client client = createClient();

            SendSmsRequest request = new SendSmsRequest()
                    .setPhoneNumbers(phoneNumber)
                    .setSignName(signName) // 需在阿里云控制台申请
                    .setTemplateCode(templateCode) // 需在阿里云控制台申请
                    .setTemplateParam("{\"code\":\"" + code + "\"}");

            SendSmsResponse response = client.sendSms(request);

            if ("OK".equals(response.getBody().getCode())) {
                System.out.println("短信发送成功");
            } else {
                System.err.println("发送失败：" + response.getBody().getMessage());
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}


