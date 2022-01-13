/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ignite/network/codec_data_filter.h>

namespace ignite
{
    namespace network
    {
        CodecDataFilter::CodecDataFilter(const SP_CodecFactory &factory) :
            codecFactory(factory),
            codecs(new CodecMap()),
            codecsCs()
        {
            // No-op.
        }

        CodecDataFilter::~CodecDataFilter()
        {
            delete codecs;
        }

        bool CodecDataFilter::Send(uint64_t id, const DataBuffer &data)
        {
            SP_Codec codec = FindCodec(id);
            if (!codec.IsValid())
                return false;

            DataBuffer data0(data);
            while (true)
            {
                DataBuffer out = codec.Get()->Encode(data0);

                if (out.IsEmpty())
                    break;

                DataFilterAdapter::Send(id, out);
            }

            return true;
        }

        void CodecDataFilter::OnConnectionSuccess(const EndPoint &addr, uint64_t id)
        {
            {
                common::concurrent::CsLockGuard lock(codecsCs);

                codecs->insert(std::make_pair(id, codecFactory.Get()->Build()));
            }

            DataFilterAdapter::OnConnectionSuccess(addr, id);
        }

        void CodecDataFilter::OnConnectionClosed(uint64_t id, const IgniteError *err)
        {
            {
                common::concurrent::CsLockGuard lock(codecsCs);

                codecs->erase(id);
            }

            DataFilterAdapter::OnConnectionClosed(id, err);
        }

        void CodecDataFilter::OnMessageReceived(uint64_t id, const DataBuffer &msg)
        {
            SP_Codec codec = FindCodec(id);
            if (!codec.IsValid())
                return;

            DataBuffer msg0(msg);
            while (true)
            {
                DataBuffer out = codec.Get()->Decode(msg0);

                if (out.IsEmpty())
                    break;

                DataFilterAdapter::OnMessageReceived(id, out);
            }
        }

        SP_Codec CodecDataFilter::FindCodec(uint64_t id)
        {
            common::concurrent::CsLockGuard lock(codecsCs);

            std::map<uint64_t, SP_Codec>::iterator it = codecs->find(id);
            if (it == codecs->end())
                return SP_Codec();

            return it->second;
        }
    }
}
