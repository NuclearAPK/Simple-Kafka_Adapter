#include <cstdlib>
#include "jnienv.h"

static JavaVM *sJavaVM = nullptr;

JNIEnv *getJniEnv() {
    JNIEnv *env = nullptr;

    switch (sJavaVM->GetEnv((void **) &env, JNI_VERSION_1_6)) {
        case JNI_OK:
            return env;

        case JNI_EDETACHED: {
            JavaVMAttachArgs args;
            args.name = nullptr;
            args.group = nullptr;
            args.version = JNI_VERSION_1_6;

            if (!sJavaVM->AttachCurrentThreadAsDaemon(&env, &args)) {
                return env;
            }
            break;
        }
    }

    return nullptr;
};

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *aJavaVM, void *aReserved) {
    sJavaVM = aJavaVM;
    return JNI_VERSION_1_6;
}

extern "C" JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *aJavaVM, void *aReserved) {
    sJavaVM = nullptr;
}