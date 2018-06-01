/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package priorities

import (
	"crypto/sha256"
	"reflect"
	"sort"
	"testing"

	"encoding/hex"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	"k8s.io/kubernetes/pkg/scheduler/schedulercache"
	"k8s.io/kubernetes/pkg/util/parsers"
)

func TestImageLocalityPriority(t *testing.T) {
	test40250 := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "gcr.io/40",
			},
			{
				Image: "gcr.io/250",
			},
		},
	}

	test40140 := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "gcr.io/40",
			},
			{
				Image: "gcr.io/140",
			},
		},
	}

	testTagged40140 := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "gcr.io/40:v1",
			},
			{
				Image: "gcr.io/140:v1",
			},
		},
	}

	testDigest40140 := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "gcr.io/40@" + getImageFakeDigest("40"),
			},
			{
				Image: "gcr.io/140@" + getImageFakeDigest("140"),
			},
		},
	}

	test40140Default := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "40:v1",
			},
			{
				Image: "140:v1",
			},
		},
	}

	testDockerIO40140 := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "docker.io/40:v1",
			},
			{
				Image: "docker.io/140:v1",
			},
		},
	}

	testDockerIOLibrary40140 := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "docker.io/library/40:v1",
			},
			{
				Image: "docker.io/library/140:v1",
			},
		},
	}

	testMinMax := v1.PodSpec{
		Containers: []v1.Container{
			{
				Image: "gcr.io/10",
			},
			{
				Image: "gcr.io/2000",
			},
		},
	}

	node401402000 := v1.NodeStatus{
		Images: []v1.ContainerImage{
			{
				Names: []string{
					"gcr.io/40:" + parsers.DefaultImageTag,
					"gcr.io/40:v1",
					"gcr.io/40:v1",
					"gcr.io/40@" + getImageFakeDigest("40"),
					"40:v1",
				},
				SizeBytes: int64(40 * mb),
			},
			{
				Names: []string{
					"gcr.io/140:" + parsers.DefaultImageTag,
					"gcr.io/140:v1",
					"gcr.io/140@" + getImageFakeDigest("140"),
					"140:v1",
				},
				SizeBytes: int64(140 * mb),
			},
			{
				Names: []string{
					"gcr.io/2000:" + parsers.DefaultImageTag,
				},
				SizeBytes: int64(2000 * mb),
			},
		},
	}

	node40140DockerIO := v1.NodeStatus{
		Images: []v1.ContainerImage{
			{
				Names: []string{
					"docker.io/40:v1",
				},
				SizeBytes: int64(40 * mb),
			},
			{
				Names: []string{
					"docker.io/140:v1",
				},
				SizeBytes: int64(140 * mb),
			},
		},
	}

	node40140DockerIOLibrary := v1.NodeStatus{
		Images: []v1.ContainerImage{
			{
				Names: []string{
					"docker.io/library/40:v1",
				},
				SizeBytes: int64(40 * mb),
			},
			{
				Names: []string{
					"docker.io/library/140:v1",
				},
				SizeBytes: int64(140 * mb),
			},
		},
	}

	node25010 := v1.NodeStatus{
		Images: []v1.ContainerImage{
			{
				Names: []string{
					"gcr.io/250:" + parsers.DefaultImageTag,
				},
				SizeBytes: int64(250 * mb),
			},
			{
				Names: []string{
					"gcr.io/10:" + parsers.DefaultImageTag,
					"gcr.io/10:v1",
				},
				SizeBytes: int64(10 * mb),
			},
		},
	}

	tests := []struct {
		pod          *v1.Pod
		pods         []*v1.Pod
		nodes        []*v1.Node
		expectedList schedulerapi.HostPriorityList
		name         string
	}{
		{
			// Pod: gcr.io/40 gcr.io/250

			// Node1
			// Image: gcr.io/40:default_tag 40MB
			// Score: (40M-23M)/97.7M + 1 = 1

			// Node2
			// Image: gcr.io/250:default_tag 250MB
			// Score: (250M-23M)/97.7M + 1 = 3
			pod:          &v1.Pod{Spec: test40250},
			nodes:        []*v1.Node{makeImageNode("machine1", node401402000), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 1}, {Host: "machine2", Score: 3}},
			name:         "two images spread on two nodes, prefer the larger image one",
		},
		{
			// Pod: gcr.io/40 gcr.io/140

			// Node1
			// Image: gcr.io/40:default_tag 40MB, gcr.io/140:default_tag 140MB
			// Score: (40M+140M-23M)/97.7M + 1 = 2

			// Node2
			// Image: not present
			// Score: 0
			pod:          &v1.Pod{Spec: test40140},
			nodes:        []*v1.Node{makeImageNode("machine1", node401402000), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 2}, {Host: "machine2", Score: 0}},
			name:         "two images on one node, prefer this node",
		},
		{
			// Pod: gcr.io/40:v1 gcr.io/140:v1

			// Node1
			// Image: gcr.io/40:v1 40MB, gcr.io/140:v1 140MB
			// Score: (40M+140M-23M)/97.7M + 1 = 2

			// Node2
			// Image: not present
			// Score: 0
			pod:          &v1.Pod{Spec: testTagged40140},
			nodes:        []*v1.Node{makeImageNode("machine1", node401402000), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 2}, {Host: "machine2", Score: 0}},
			name:         "two images on one node, prefer this node",
		},
		{
			// Pod: gcr.io/40@digest gcr.io/140@digest

			// Node1
			// Image: gcr.io/40@digest 40MB, gcr.io/140@digest 140MB
			// Score: (40M+140M-23M)/97.7M + 1 = 2

			// Node2
			// Image: not present
			// Score: 0
			pod:          &v1.Pod{Spec: testDigest40140},
			nodes:        []*v1.Node{makeImageNode("machine1", node401402000), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 2}, {Host: "machine2", Score: 0}},
			name:         "two images on one node, prefer this node",
		},
		{
			// Pod: docker.io/40:v1 docker.io/140:v1

			// Node1
			// Image: docker.io/library/40:v1 40MB, docker.io/library/140:v1 140MB
			// Score: (40M+140M-23M)/97.7M + 1 = 2

			// Node2
			// Image: not present
			// Score: 0
			pod:          &v1.Pod{Spec: testDockerIO40140},
			nodes:        []*v1.Node{makeImageNode("machine1", node40140DockerIOLibrary), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 2}, {Host: "machine2", Score: 0}},
			name:         "two images on one node, prefer this node",
		},
		{
			// Pod: docker.io/40:v1 docker.io/140:v1

			// Node1
			// Image: 40:v1 40MB, 140:v1 140MB
			// Score: (40M+140M-23M)/97.7M + 1 = 2

			// Node2
			// Image: not present
			// Score: 0
			pod:          &v1.Pod{Spec: testDockerIO40140},
			nodes:        []*v1.Node{makeImageNode("machine1", node401402000), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 2}, {Host: "machine2", Score: 0}},
			name:         "two images on one node, prefer this node",
		},
		{
			// Pod: docker.io/library/40:v1 docker.io/library/140:v1

			// Node1
			// Image: 40:v1 40MB, 140:v1 140MB
			// Score: (40M+140M-23M)/97.7M + 1 = 2

			// Node2
			// Image: not present
			// Score: 0
			pod:          &v1.Pod{Spec: testDockerIOLibrary40140},
			nodes:        []*v1.Node{makeImageNode("machine1", node401402000), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 2}, {Host: "machine2", Score: 0}},
			name:         "two images on one node, prefer this node",
		},
		{
			// Pod: 40:v1 140:v1

			// Node1
			// Image: docker.io/library/40:v1 40MB, docker.io/library/140:v1 140MB
			// Score: (40M+140M-23M)/97.7M + 1 = 2

			// Node2
			// Image: not present
			// Score: 0
			pod:          &v1.Pod{Spec: test40140Default},
			nodes:        []*v1.Node{makeImageNode("machine1", node40140DockerIOLibrary), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 2}, {Host: "machine2", Score: 0}},
			name:         "two images on one node, prefer this node",
		},
		{
			// Pod: 40:v1 140:v1

			// Node1
			// Image: docker.io/40:v1 40MB, docker.io/140:v1 140MB
			// Score: (40M+140M-23M)/97.7M + 1 = 2

			// Node2
			// Image: not present
			// Score: 0
			pod:          &v1.Pod{Spec: test40140Default},
			nodes:        []*v1.Node{makeImageNode("machine1", node40140DockerIO), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 2}, {Host: "machine2", Score: 0}},
			name:         "two images on one node, prefer this node",
		},
		{
			// Pod: gcr.io/2000 gcr.io/10

			// Node1
			// Image: gcr.io/2000:default_tag 2000MB
			// Score: 2000 > max score = 10

			// Node2
			// Image: gcr.io/10:default_tag 10MB
			// Score: 10 < min score = 0
			pod:          &v1.Pod{Spec: testMinMax},
			nodes:        []*v1.Node{makeImageNode("machine1", node401402000), makeImageNode("machine2", node25010)},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: schedulerapi.MaxPriority}, {Host: "machine2", Score: 0}},
			name:         "if exceed limit, use limit",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeNameToInfo := schedulercache.CreateNodeNameToInfoMap(test.pods, test.nodes)
			list, err := priorityFunction(ImageLocalityPriorityMap, nil, nil)(test.pod, nodeNameToInfo, test.nodes)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			sort.Sort(test.expectedList)
			sort.Sort(list)

			if !reflect.DeepEqual(test.expectedList, list) {
				t.Errorf("expected %#v, got %#v", test.expectedList, list)
			}
		})
	}
}

func makeImageNode(node string, status v1.NodeStatus) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: node},
		Status:     status,
	}
}

func getImageFakeDigest(fakeContent string) string {
	hash := sha256.Sum256([]byte(fakeContent))
	return "sha256:" + hex.EncodeToString(hash[:])
}
