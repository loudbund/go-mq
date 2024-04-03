package service

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

// BNHelper bucket名称管理工具
type BNHelper struct{}

// GetBucketStartKey 获取bucket开始key值
// 说明: 修改该值，只会影响还未创建的数据bucket，该值需要1打头，该值不可太小，否则会影响表数据顺序
func (bn *BNHelper) GetBucketStartKey() int64 {
	return 100000000
}

// GetBucketNameFromTime 时间格式 转换成 bucket名称
// 参数：时间格式为 20231212235959 (年月日时分秒)
// 返回格式：D加11位时间，及精确到10分钟 (D年月日时分<十位部分>)
func (bn *BNHelper) GetBucketNameFromTime(tim string) string {
	return "D" + tim[0:11]
}

// GetBucketName 获取bucket名称，
// 参数：有时间参数则通过时间参数转换，否则取当前时间转换的
// 返回格式：D加11位时间，及精确到10分钟 (D年月日时分<十位部分>)
func (bn *BNHelper) GetBucketName(t ...time.Time) string {
	if len(t) > 0 {
		return "D" + t[0].Format("200601021504")[0:11]
	} else {
		return "D" + time.Now().Format("200601021504")[0:11]
	}
}

// MinBucketName 当前最小有效bucket名
func (bn *BNHelper) MinBucketName() string {
	tDiff, _ := time.ParseDuration(fmt.Sprintf("%dh", -CfgBoltDb.HourDataRetain))
	return (&BNHelper{}).GetBucketName(time.Now().Add(tDiff))
}

// NextBucketName 下一个bucket名称
func (bn *BNHelper) NextBucketName(bucketName string) string {
	if bucketName < bn.MinBucketName() {
		return bn.MinBucketName()
	} else if bucketName > bn.GetBucketName() {
		return bucketName
	} else {
		s := bucketName
		at, _ := time.ParseInLocation("2006-01-02 15:04:05",
			fmt.Sprintf("%s-%s-%s %s:%s0:00", s[1:5], s[5:7], s[7:9], s[9:11], s[11:12]),
			time.Local)
		tDiff, _ := time.ParseDuration(fmt.Sprintf("%dm", 10))
		return bn.GetBucketName(at.Add(tDiff))
	}
}

// CheckBucketName 检查bucket名称是否有效
func (bn *BNHelper) CheckBucketName(bucketName string) bool {
	tDiff, _ := time.ParseDuration(fmt.Sprintf("%dh", -CfgBoltDb.HourDataRetain))
	minBucketName := (&BNHelper{}).GetBucketName(time.Now().Add(tDiff))
	nowBucketName := bn.GetBucketName()
	return bucketName >= minBucketName && bucketName <= nowBucketName
}

// 编码
func gobEncode(da interface{}) []byte {
	var result bytes.Buffer //缓冲区
	encoder := gob.NewEncoder(&result)
	_ = encoder.Encode(da)
	return result.Bytes()
}

// 解码
func gobDecode(bv []byte, ret interface{}) error {
	var b bytes.Buffer //缓冲区
	b.Write(bv)
	ca := gob.NewDecoder(&b)
	if err := ca.Decode(ret); err != nil {
		return err
	}
	return nil
}
