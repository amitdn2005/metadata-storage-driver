package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	// "sync"
	"v2/model"

	"github.com/aws/aws-sdk-go/aws/session"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/storage"
)

type AwsAdapter struct {
	Session *session.Session
}

func main() {

	client, ctx1, _ := CreateDriverGCP()
	buckArr1, _ := BucketListGCP(&client, ctx1, "640353178356")
	fmt.Print("AMIT:::")
	fmt.Print(len(buckArr1))

	out, _ := json.MarshalIndent(buckArr1, "", "   ")
	err := ioutil.WriteFile("metadata.json", out, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func CreateDriverGCP() (storage.Client, context.Context, error) {
	ctx1 := context.Background()
	client, err := storage.NewClient(ctx1)
	if err != nil {
		return *client, nil, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	return *client, ctx1, nil
}

func BucketListGCP(gcc *storage.Client, ctx context.Context, projectNumber string) ([]model.MetaBucket, error) {

	var err error
	bucketArray := make([]model.MetaBucket, 0)
	fmt.Printf("gcc: %v\n", gcc)
	it := gcc.Buckets(ctx, projectNumber)
	fmt.Printf("it: %v\n", it)
	i := 0
	for {
		bucket, err := it.Next()
		i = i + 1
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		go iterateBuckets(i, gcc, ctx, projectNumber, bucket, bucketArray)

		fmt.Print("AMIT:::BUCKET Name " + bucket.Name + "\n")
		fmt.Printf("bucket: %v\n", bucket)

	}

	// wg.Wait()

	return bucketArray, err
}

// func iterateObjects(i int, object *s3.Object, svc *s3.S3, bucketName string, objectArray []model.MetaObject, wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	obj := model.MetaObject{}
// 	obj.LastModifiedDate = *object.LastModified
// 	obj.ObjectName = *object.Key
// 	obj.Size = *object.Size
// 	obj.StorageClass = *object.StorageClass

// 	meta, err := svc.HeadObject(&s3.HeadObjectInput{Bucket: &bucketName, Key: object.Key})
// 	if err != nil {
// 		log.Errorf("cannot perform head object on object %v in bucket %v. failed with error: %v\n", *object.Key, bucketName, err)
// 	}
// 	if meta.ServerSideEncryption != nil {
// 		obj.ServerSideEncryption = *meta.ServerSideEncryption
// 	}
// 	if meta.VersionId != nil {
// 		obj.VersionId = *meta.VersionId
// 	}
// 	obj.ObjectType = *meta.ContentType
// 	if meta.Expires != nil {
// 		expiresTime, err := time.Parse(time.RFC3339, *meta.Expires)
// 		if err != nil {
// 			log.Errorf("unable to parse given string to time type. error: %v. skipping ExpiresDate field\n", err)
// 		} else {
// 			obj.ExpiresDate = expiresTime
// 		}
// 	}
// 	if meta.ReplicationStatus != nil {
// 		obj.ReplicationStatus = *meta.ReplicationStatus
// 	}
// 	objectArray[i] = obj
// }

func ObjectListGCP(gcc *storage.Client, ctx context.Context, projectNumber string, bucket *model.MetaBucket) error {
	fmt.Printf("\nbucket.Name: %v\n", bucket.Name)
	objectIterator := gcc.Bucket(bucket.Name).Objects(ctx, nil)
	fmt.Printf("\nobjectIterator: %v\n", objectIterator)

	objectArray := make([]model.MetaObject, 1)
	i := 0
	for {
		object, err := objectIterator.Next()
		fmt.Printf("object: %v\n", object)
		fmt.Print("AMIT:::Object Name " + object.Name + "\n")
		i = i + 1
		// go iterateObjects(i, object, svc, bucket.Name, objectArray)

		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("Bucket(%q).Objects: %v", bucket, err)
		}
		fmt.Print("AMIT:::Object Name " + object.Name + "\n")
		fmt.Printf("object: %v\n", object)

	}
	bucket.NumberOfObjects = int64(i)
	bucket.Objects = objectArray
	return nil
}

func iterateBuckets(i int, gcc *storage.Client, ctx context.Context, projectNumber string, bucket *storage.BucketAttrs, bucketArray []model.MetaBucket) {
	buck := model.MetaBucket{}
	buck.CreationDate = &bucket.Created
	buck.Name = bucket.Name
	// loc, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: bucket.Name})
	// if err != nil {
	// 	log.Errorf("unable to get bucket location. failed with error: %v\n", err)
	// } else {
	// 	buck.Region = *loc.LocationConstraint
	// }
	// newSvc := s3.New(sess, aws.NewConfig().WithRegion(buck.Region))
	// tags, err := newSvc.GetBucketTagging(&s3.GetBucketTaggingInput{Bucket: bucket.Name})

	// if err != nil && !strings.Contains(err.Error(), "NoSuchTagSet") {
	// 	log.Errorf("unable to get bucket tags. failed with error: %v\n", err)
	// } else {
	// 	tagset := make(map[string]string)
	// 	for _, tag := range tags.TagSet {
	// 		tagset[*tag.Key] = *tag.Value
	// 	}
	// 	buck.BucketTags = tagset
	// }
	err := ObjectListGCP(gcc, ctx, projectNumber, &buck)
	if err != nil {
		fmt.Printf("error while collecting object metadata for bucket %v. failed with error: %v\n", buck.Name, err)
	}
	bucketArray[i] = buck
}
